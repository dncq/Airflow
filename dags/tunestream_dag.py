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

# Constants
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

# def load_json_to_postgres(table, path, columns, **kwargs):
#     from airflow.hooks.base import BaseHook

#     conn = BaseHook.get_connection(POSTGRES_CONN_ID)
#     pg_conn = psycopg2.connect(
#         host=conn.host,
#         dbname=conn.schema,
#         user=conn.login,
#         password=conn.password,
#         port=conn.port
#     )
#     cursor = pg_conn.cursor()

#     for root, _, files in os.walk(path):
#         for file in files:
#             if not file.endswith('.json'):
#                 continue
#             filepath = os.path.join(root, file)
#             with open(filepath) as f:
#                 try:
#                     data = json.load(f)
#                 except json.JSONDecodeError as e:
#                     print(f"Skipping invalid JSON {filepath}: {e}")
#                     continue
#                 values = [data.get(col) for col in columns]
#                 placeholders = ','.join(['%s'] * len(columns))
#                 query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
#                 cursor.execute(query, values)

#     pg_conn.commit()
#     cursor.close()
#     pg_conn.close()


# def run_quality_checks(**kwargs):
#     from airflow.hooks.base import BaseHook

#     conn = BaseHook.get_connection(POSTGRES_CONN_ID)
#     pg_conn = psycopg2.connect(
#         host=conn.host,
#         dbname=conn.schema,
#         user=conn.login,
#         password=conn.password,
#         port=conn.port
#     )
#     cursor = pg_conn.cursor()

#     checks = {
#         "songplays": "SELECT COUNT(*) FROM songplays;",
#         "users": "SELECT COUNT(*) FROM users WHERE userid IS NULL;"
#     }

#     for table, query in checks.items():
#         cursor.execute(query)
#         result = cursor.fetchone()
#         if result[0] == 0:
#             raise ValueError(f"Data quality check failed for {table}: no records found.")
#         print(f"Data quality check passed for {table}: {result[0]} records.")

#     cursor.close()
#     pg_conn.close()

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

    # Task dependencies
    begin_execution >> [stage_songs, stage_events] >> load_songplays_fact_table
    load_songplays_fact_table >> [
        load_user_dim_table,
        load_song_dim_table,
        load_artist_dim_table,
        load_time_dim_table
    ] >> run_data_quality_checks >> end_execution
