from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Pratul',
    'start_date': datetime(2018, 10, 1),
    'end_date': datetime(2018, 12, 30),
    'depends_on_past':False
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':3,
    'retry_delay':timedelta(minutes=5)
}

dag = DAG('sparkify_etl',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
    region="us-west-2",
    json="s3://udacity-dend/log_json_path.json"   
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
    region="us-west-2",
    json="s3://udacity-dend/log_json_path.json"   
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songplays",
    sql_template=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.users",
    sql_template=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songs",
    sql_template=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.artists",
    sql_template=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.time",
    sql_template=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table_list=["public.songplays", 
            "public.songs",
            "public.users",
            "public.artists",
            "public.time"]
    redshift_conn_id="redshift",
    sql_template="select count(*) from {}"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#--- Create the DAG ----

start_operator>>stage_events_to_redshift >> load_songplays_table
start_operator>>stage_songs_to_redshift >> load_songplays_table


load_songplays_table>>load_user_dimension_table >>run_quality_checks
load_songplays_table>>load_song_dimension_table >>run_quality_checks
load_songplays_table>>load_artist_dimension_table >>run_quality_checks
load_songplays_table>>load_time_dimension_table >>run_quality_checks


run_quality_checks>>end_operator