# from airflow.hooks.base import BaseHook
# import psycopg2


# POSTGRES_CONN_ID = 'postgres_local'

# def run_quality_checks(**kwargs):

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

from airflow.hooks.base import BaseHook
import psycopg2

POSTGRES_CONN_ID = 'postgres_local'

def run_quality_checks(**kwargs):
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_conn = psycopg2.connect(
        host=conn.host,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )
    cursor = pg_conn.cursor()

    # Define quality checks per table
    checks = {
        "songplays": {
            "empty_check": "SELECT COUNT(*) FROM songplays;",
            "null_check": "SELECT COUNT(*) FROM songplays WHERE playid IS NULL;",
            "unique_check": "SELECT COUNT(*) - COUNT(DISTINCT playid) FROM songplays;"
        },
        "users": {
            "empty_check": "SELECT COUNT(*) FROM users;",
            "null_check": "SELECT COUNT(*) FROM users WHERE userid IS NULL;",
            "unique_check": "SELECT COUNT(*) - COUNT(DISTINCT userid) FROM users;"
        },
        "songs": {
            "empty_check": "SELECT COUNT(*) FROM songs;",
            "null_check": "SELECT COUNT(*) FROM songs WHERE songid IS NULL;",
            "unique_check": "SELECT COUNT(*) - COUNT(DISTINCT songid) FROM songs;"
        },
        "artists": {
            "empty_check": "SELECT COUNT(*) FROM artists;",
            "null_check": "SELECT COUNT(*) FROM artists WHERE artistid IS NULL;",
            "unique_check": "SELECT COUNT(*) - COUNT(DISTINCT artistid) FROM artists;"
        },
        "time": {
            "empty_check": "SELECT COUNT(*) FROM time;",
            "null_check": "SELECT COUNT(*) FROM time WHERE start_time IS NULL;",
            "unique_check": "SELECT COUNT(*) - COUNT(DISTINCT start_time) FROM time;"
        }
    }

    for table, queries in checks.items():
        print(f"Running data quality checks for table: {table}")

        # 1. Empty Table Check
        cursor.execute(queries["empty_check"])
        count = cursor.fetchone()[0]
        if count == 0:
            raise ValueError(f"[FAILED] {table} is empty!")
        print(f"[PASSED] {table} has {count} records.")

        # 2. NULL Check
        cursor.execute(queries["null_check"])
        null_count = cursor.fetchone()[0]
        if null_count > 0:
            raise ValueError(f"[FAILED] {table} has {null_count} NULL values in primary column!")
        print(f"[PASSED] {table} has no NULLs in primary column.")

        # 3. Uniqueness Check
        cursor.execute(queries["unique_check"])
        duplicates = cursor.fetchone()[0]
        if duplicates > 0:
            raise ValueError(f"[FAILED] {table} has {duplicates} duplicate values in primary column!")
        print(f"[PASSED] {table} has unique primary column values.")

    cursor.close()
    pg_conn.close()
