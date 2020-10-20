import configparser
import psycopg2
import os
import unittest

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.taskinstance import TaskInstance

from airflow_etl_template.constants import DIR_DATA_TEST, CONFIG_PATH_DWH_CURRENT, VIZ_TEST, logging
from airflow_etl_template.redshift.src import sql_queries, utils
from airflow_etl_template.redshift.tests import utils_tests
from airflow_etl_template.redshift.scripts import create_tables
from airflow_etl_template.airflow.src.plugins.operators.data_quality import DataQualityOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TestQualityCheck(unittest.TestCase):
    """Test that operator correctly executes checks over the data
    """

    def setUp(self):
        logging.info("Connecting to Redshift")
        config = configparser.ConfigParser()
        config.read(CONFIG_PATH_DWH_CURRENT)
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        create_tables.drop_tables(cur, conn)
        create_tables.create_tables(cur, conn)
        conn.commit()
        logging.info("Copying data into Staging tables..")
        df_log = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_songplays_staging_events.csv"))
        df_songs = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_songplays_staging_songs.csv"))
        df_log_ins = utils_tests.create_and_fill_log_staging_from_dataframe(cur, df_log, viz=VIZ_TEST)
        df_songs_ins = utils_tests.create_and_fill_songs_staging_from_dataframe(cur, df_songs, viz=VIZ_TEST)
        logging.info("Filling star schema..")
        cur.execute(sql_queries.songplay_table_insert)
        cur.execute(sql_queries.user_table_insert)
        cur.execute(sql_queries.time_table_insert)
        cur.execute(sql_queries.song_table_insert)
        cur.execute(sql_queries.artist_table_insert)
        conn.commit()
        conn.close()

    def tearDown(self):
        logging.info("Deleting star schema..")
        config = configparser.ConfigParser()
        config.read(CONFIG_PATH_DWH_CURRENT)
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        create_tables.drop_tables(cur, conn)

    def test_quality_check(self):
        dag = DAG(dag_id='test_data_check', start_date=datetime.now())
        task = DataQualityOperator(
            dag=dag,
            task_id='data_quality_check',
            conn_id="redshift",
            dq_checks=sql_queries.dq_checks
        )
        ti = TaskInstance(task=task, execution_date=datetime.now())
        task.execute(ti.get_template_context())
