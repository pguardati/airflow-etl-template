import boto3
import configparser
import psycopg2

import unittest
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.taskinstance import TaskInstance

from airflow_etl_template.redshift.scripts import create_tables
from airflow_etl_template.redshift.src import utils, sql_queries
from airflow_etl_template.redshift.tests import utils_tests
from airflow_etl_template.constants import CONFIG_PATH_DWH_CURRENT, VIZ_TEST, logging
from airflow_etl_template.airflow.src.plugins.operators.stage_redshift import StageToRedshiftOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TestStagingOperator(unittest.TestCase):
    """Test loading from s3 into staging tables
    """
    def setUp(self):
        logger.info("Reading aws Configuration..")
        config = configparser.ConfigParser()
        config.read(CONFIG_PATH_DWH_CURRENT)
        KEY = config.get("AWS", "KEY")
        SECRET = config.get("AWS", "SECRET")
        logger.info("Reset tables..")
        s3 = boto3.resource(
            "s3",
            region_name="us-west-2",
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
        )
        self.conn = psycopg2.connect(
            "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        self.cur = self.conn.cursor()
        create_tables.drop_tables(self.cur, self.conn)
        create_tables.create_tables(self.cur, self.conn)
        logger.info("Creating test dag..")
        self.dag = DAG(
            dag_id='test_load_dimension',
            start_date=datetime.now()
        )

    def test_copy_staging_events(self):
        logger.info("Executing dag to copy data into staging events table..")
        task = StageToRedshiftOperator(
            dag=self.dag,
            task_id='stage_events',
            redshift_conn_id="redshift",
            aws_credentials_id="aws_credentials",
            table="staging_events",
            s3_path="s3://udacity-dend/log_data/2018/11/2018-11",
            json_format="s3://udacity-dend/log_json_path.json"
        )
        ti = TaskInstance(task=task, execution_date=datetime.now())
        task.execute(ti.get_template_context())
        logger.info("Checking data have been copied..")
        _ = utils.get_top_elements_from_table(self.cur, "staging_events", viz=VIZ_TEST)

    def test_copy_staging_songs(self):
        logger.info("Executing dag to copy data into staging songs table..")
        task = StageToRedshiftOperator(
            dag=self.dag,
            task_id='stage_songs',
            redshift_conn_id="redshift",
            aws_credentials_id="aws_credentials",
            table="staging_songs",
            s3_path="s3://udacity-dend/song_data/A/A",
            json_format="auto"
        )
        ti = TaskInstance(task=task, execution_date=datetime.now())
        task.execute(ti.get_template_context())
        logger.info("Checking data have been copied..")
        _ = utils.get_top_elements_from_table(self.cur, "staging_songs", viz=VIZ_TEST)


if __name__ == "__main__":
    unittest.main()
