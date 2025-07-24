from airflow.hooks.base import BaseHook

import os
import json
import psycopg2


POSTGRES_CONN_ID = 'postgres_local'

# def load_json_to_postgres(table, path, columns, **kwargs):

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

def load_json_to_postgres(table, path, columns, **kwargs):

    # Connect to Postgres via Airflow connection
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_conn = psycopg2.connect(
        host=conn.host,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )
    cursor = pg_conn.cursor()
    total_files = 0
    inserted_rows = 0

    print(f"[DEBUG] Starting loader for table `{table}` at path: {path}")

    for root, dirs, files in os.walk(path):
        print(f"[DEBUG] Scanning directory: {root}, contains {len(files)} files, {len(dirs)} subdirs")
        for file in files:
            total_files += 1
            if not file.endswith('.json'):
                print(f"[DEBUG] Skipping non-JSON file: {file}")
                continue

            filepath = os.path.join(root, file)
            print(f"[DEBUG] Found JSON file: {filepath}")
            try:
                with open(filepath) as f:
                    data = json.load(f)
                print(f"[DEBUG] Parsed JSON with keys: {list(data.keys())}")

                values = [data.get(col, None) for col in columns]
                placeholders = ','.join(['%s'] * len(columns))
                query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
                cursor.execute(query, values)
                inserted_rows += 1
            except json.JSONDecodeError as e:
                print(f"[ERROR] JSON decode failed for {filepath}: {e}")
            except Exception as e:
                print(f"[ERROR] DB insert error for {filepath}: {e}")

    pg_conn.commit()
    cursor.close()
    pg_conn.close()

    print(f"[DEBUG] Loader finished: total JSON files scanned: {total_files}, rows inserted: {inserted_rows}")
    if inserted_rows == 0:
        raise ValueError(f"No rows inserted into {table} — check data path or loader logic.")
