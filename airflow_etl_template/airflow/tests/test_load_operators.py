import configparser
import psycopg2
import os
import unittest

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.taskinstance import TaskInstance

from airflow_etl_template.constants import DIR_DATA_TEST, CONFIG_PATH_DWH_CURRENT, VIZ_TEST
from airflow_etl_template.redshift.src import sql_queries, utils
from airflow_etl_template.redshift.tests import utils_tests
from airflow_etl_template.airflow.src.plugins.operators.load_fact import LoadFactOperator
from airflow_etl_template.airflow.src.plugins.operators.load_dimension import LoadDimensionOperator
from airflow_etl_template.redshift.scripts import create_tables

config = configparser.ConfigParser()
config.read(CONFIG_PATH_DWH_CURRENT)


class TestLoad(unittest.TestCase):
    """Test transfer of data from staging tables to table of the star schema """
    def setUp(self):
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        create_tables.drop_tables(cur, conn)
        create_tables.create_tables(cur, conn)
        conn.commit()
        conn.close()

    def tearDown(self):
        pass

    def test_load_dim_from_events(self):
        """Operator transfers data from staging events into user table"""
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        # create staging table
        df_log = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_user_staging_events.csv"))
        df_log_ins = utils_tests.create_and_fill_log_staging_from_dataframe(cur, df_log, viz=VIZ_TEST)
        # reset user table
        cur.execute(sql_queries.user_table_drop)
        cur.execute(sql_queries.user_table_create)
        conn.commit()

        # test operator
        dag = DAG(dag_id='test_load_dimension', start_date=datetime.now())
        task = LoadDimensionOperator(
            dag=dag,
            task_id='load_users_table',
            conn_id="redshift",
            table="users",
            sql_copy_cmd=sql_queries.user_table_insert,
            truncate=True
        )
        ti = TaskInstance(task=task, execution_date=datetime.now())
        task.execute(ti.get_template_context())

        # check table is not empty
        df_users = utils.get_top_elements_from_table(cur, "users", 10, viz=VIZ_TEST)
        conn.close()

    def test_load_dim_from_songs(self):
        """Operator transfers data from songs events into song table"""
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        # create staging table
        df_songs = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_songs_staging_songs.csv"))
        df_songs_ins = utils_tests.create_and_fill_songs_staging_from_dataframe(cur, df_songs, viz=VIZ_TEST)
        # reset user table
        cur.execute(sql_queries.song_table_drop)
        cur.execute(sql_queries.song_table_create)
        conn.commit()

        # test operator
        dag = DAG(dag_id='test_load_dimension', start_date=datetime.now())
        task = LoadDimensionOperator(
            dag=dag,
            task_id='load_songs_table',
            conn_id="redshift",
            table="songs",
            sql_copy_cmd=sql_queries.song_table_insert,
            truncate=True
        )
        ti = TaskInstance(task=task, execution_date=datetime.now())
        task.execute(ti.get_template_context())

        # check table is not empty
        df_users = utils.get_top_elements_from_table(cur, "songs", 10, viz=VIZ_TEST)
        conn.close()

    def test_load_fact(self):
        """Operator transfers data from staging songs and events into songplays table"""
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        # create staging table
        df_log = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_songplays_staging_events.csv"))
        df_songs = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_songplays_staging_songs.csv"))
        df_log_ins = utils_tests.create_and_fill_log_staging_from_dataframe(cur, df_log, viz=VIZ_TEST)
        df_songs_ins = utils_tests.create_and_fill_songs_staging_from_dataframe(cur, df_songs, viz=VIZ_TEST)

        # reset user table
        cur.execute(sql_queries.songplay_table_drop)
        cur.execute(sql_queries.songplay_table_create)
        conn.commit()

        # test operator
        dag = DAG(dag_id='test_load_fact', start_date=datetime.now())
        task = LoadFactOperator(
            dag=dag,
            task_id='load_songplays',
            conn_id="redshift",
            table="songplays",
            sql_copy_cmd=sql_queries.songplay_table_insert,
        )
        ti = TaskInstance(task=task, execution_date=datetime.now())
        task.execute(ti.get_template_context())

        # check table is not empty
        df_songplays = utils.get_top_elements_from_table(cur, "songplays", 10, viz=VIZ_TEST)
        conn.close()


if __name__ == "__main__":
    unittest.main()
