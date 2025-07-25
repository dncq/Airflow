from airflow.hooks.base import BaseHook

import os
import json
import psycopg2


POSTGRES_CONN_ID = 'postgres_local'

def load_json_to_postgres(table, path, columns, **kwargs):
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_conn = psycopg2.connect(
        host=conn.host,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )
    cursor = pg_conn.cursor()
    total_files = inserted_rows = 0

    print(f"Starting loader for table `{table}` at path: {path}")

    for root, _, files in os.walk(path):
        print(f"Scanning directory: {root}, contains {len(files)} files")
        for file in files:
            total_files += 1
            if not file.lower().endswith('.json'):
                print(f"Skipping non-JSON file: {file}")
                continue

            filepath = os.path.join(root, file)
            print(f"Found JSON file: {filepath}")
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    events = []
                    try:
                        obj = json.loads(content)
                
                        if isinstance(obj, list):
                            events = obj
                        else:
                            events = [obj]
                        print("Parsed as standard JSON")
                    except json.JSONDecodeError:
                        print("Fallback to NDJSON (line-by-line)")
                        events = []
                        for line in content.splitlines():
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                events.append(json.loads(line))
                            except json.JSONDecodeError as e_line:
                                print(f" Skipping invalid line: {e_line}")
                        print(f"Parsed {len(events)} JSON lines")
                # print("Parsed JSON content, processing events...")
                print(events) 
                for event in events:
                    values = [event.get(col) for col in columns ]
                    placeholders = ','.join(['%s'] * len(columns))
                    query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
                    cursor.execute(query, values)
                    inserted_rows += 1

            except Exception as e:
                print(f"[ERROR] processing {filepath}: {e}")

    pg_conn.commit()
    cursor.close()
    pg_conn.close()

    print(f"Loader finished: scanned {total_files} files, inserted {inserted_rows} rows.")
    if inserted_rows == 0:
        raise ValueError(f"No rows inserted into {table} — check data path or loader logic.")

