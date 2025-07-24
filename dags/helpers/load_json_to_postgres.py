from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os
import json
import psycopg2
from helpers.sql_queries import SqlQueries

POSTGRES_CONN_ID = 'postgres_local'

def load_json_to_postgres(table, path, columns, **kwargs):
    # from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_conn = psycopg2.connect(
        host=conn.host,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )
    cursor = pg_conn.cursor()

    for root, _, files in os.walk(path):
        for file in files:
            if not file.endswith('.json'):
                continue
            filepath = os.path.join(root, file)
            with open(filepath) as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON {filepath}: {e}")
                    continue
                values = [data.get(col) for col in columns]
                placeholders = ','.join(['%s'] * len(columns))
                query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
                cursor.execute(query, values)

    pg_conn.commit()
    cursor.close()
    pg_conn.close()
