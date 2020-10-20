from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow_etl_template.redshift.src import sql_queries
from airflow_etl_template.airflow.src.plugins.operators.stage_redshift import StageToRedshiftOperator
from airflow_etl_template.airflow.src.plugins.operators.load_fact import LoadFactOperator
from airflow_etl_template.airflow.src.plugins.operators.load_dimension import LoadDimensionOperator
from airflow_etl_template.airflow.src.plugins.operators.data_quality import DataQualityOperator

"""Direct Acyclic Graph that codes the ETL process
from s3 to staging tables,
and from staging tables into a star schema
"""

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 1, 10),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'catchup': False
}

dag = DAG('TemplateDataPipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False
          )

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    dag=dag,
    task_id='stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_path="s3://udacity-dend/log_data/",
    json_format="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    dag=dag,
    task_id='stage_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_path="s3://udacity-dend/song_data/",
    json_format="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id="redshift",
    table="songplays",
    sql_copy_cmd=sql_queries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id="redshift",
    table="users",
    sql_copy_cmd=sql_queries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id="redshift",
    table="songs",
    sql_copy_cmd=sql_queries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id="redshift",
    table="artists",
    sql_copy_cmd=sql_queries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id="redshift",
    table="time",
    sql_copy_cmd=sql_queries.time_table_insert,
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    conn_id="redshift",
    dag=dag,
    dq_checks=sql_queries.dq_checks
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
