from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import psycopg2
from helpers.sql_queries import SqlQueries
from helpers.load_json_to_postgres import load_json_to_postgres
from helpers.quality_checks import run_quality_checks


SONG_DATA_PATH = '/opt/airflow/data/song_data'
LOG_DATA_PATH = '/opt/airflow/data/log_data'
POSTGRES_CONN_ID = 'postgres_local'

default_args = {
    'owner': 'tunestream',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG(
    'etl_pipeline_local_postgres',
    default_args=default_args,
    description='ETL pipeline to load JSON song/log data into local PostgreSQL',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['etl', 'postgres']
)

with dag:
    begin_execution = EmptyOperator(task_id='Begin_execution')

    stage_songs = PythonOperator(
        task_id='Stage_songs',
        python_callable=load_json_to_postgres,
        op_kwargs={
            'table': 'staging_songs',
            'path': SONG_DATA_PATH,
            'columns': [
                'num_songs', 'artist_id', 'artist_name', 'artist_latitude',
                'artist_longitude', 'artist_location', 'song_id', 'title',
                'duration', 'year'
            ]
        }
    )

    stage_events = PythonOperator(
        task_id='Stage_events',
        python_callable=load_json_to_postgres,
        op_kwargs={
            'table': 'staging_events',
            'path': LOG_DATA_PATH,
            'columns': [
                'artist', 'auth', 'firstname', 'gender', 'iteminsession', 'lastname',
                'length', 'level', 'location', 'method', 'page', 'registration',
                'sessionid', 'song', 'status', 'ts', 'useragent', 'userid'
            ]
        }
    )

    load_songplays_fact_table = SQLExecuteQueryOperator(
        task_id='Load_songplays_fact_table',
        conn_id=POSTGRES_CONN_ID,
        sql=SqlQueries.songplay_table_insert,
    )

    load_user_dim_table = SQLExecuteQueryOperator(
        task_id='Load_user_dim_table',
        conn_id=POSTGRES_CONN_ID,
        sql=SqlQueries.user_table_insert,
    )

    load_song_dim_table = SQLExecuteQueryOperator(
        task_id='Load_song_dim_table',
        conn_id=POSTGRES_CONN_ID,
        sql=SqlQueries.song_table_insert,
    )

    load_artist_dim_table = SQLExecuteQueryOperator(
        task_id='Load_artist_dim_table',
        conn_id=POSTGRES_CONN_ID,
        sql=SqlQueries.artist_table_insert,
    )

    load_time_dim_table = SQLExecuteQueryOperator(
        task_id='Load_time_dim_table',
        conn_id=POSTGRES_CONN_ID,
        sql=SqlQueries.time_table_insert,
    )

    run_data_quality_checks = PythonOperator(
        task_id='Run_data_quality_checks',
        python_callable=run_quality_checks
    )

    end_execution = EmptyOperator(task_id='End_execution')

    begin_execution >> [stage_songs, stage_events] >> load_songplays_fact_table
    load_songplays_fact_table >> [
        load_user_dim_table,
        load_song_dim_table,
        load_artist_dim_table,
        load_time_dim_table
    ] >> run_data_quality_checks >> end_execution
