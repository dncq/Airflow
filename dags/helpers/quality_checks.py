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

def run_quality_checks(**kwargs):
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

    checks = {
        "songplays": "SELECT COUNT(*) FROM songplays;",
        "users": "SELECT COUNT(*) FROM users WHERE userid IS NULL;"
    }

    for table, query in checks.items():
        cursor.execute(query)
        result = cursor.fetchone()
        if result[0] == 0:
            raise ValueError(f"Data quality check failed for {table}: no records found.")
        print(f"Data quality check passed for {table}: {result[0]} records.")

    cursor.close()
    pg_conn.close()