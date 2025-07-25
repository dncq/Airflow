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
                print("Parsed JSON content, processing events...")
                print(events) 
                for event in events:
                    values = [event.get(col) for col in columns ]
                    print(values)
                    
                    lower_case_columns = [col.lower() for col in columns]
                    if "year" in columns:
                        placeholders = ','.join(['%s'] * len(columns))
                        query = f"INSERT INTO {table} ({','.join(lower_case_columns)}) VALUES ({placeholders})"
                        cursor.execute(query, values)
                        inserted_rows += 1
                    else:
                        query = """
                        INSERT INTO staging_events (
                            artist, auth, firstname, gender, iteminsession, lastname, length,
                            level, location, method, page, registration, sessionid, song,
                            status, ts, useragent, userid
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """
                        cursor.execute(query, (
                                event.get('artist'),
                                event.get('auth'),
                                event.get('firstName'),
                                event.get('gender'),
                                event.get('itemInSession'),
                                event.get('lastName'),
                                event.get('length'),
                                event.get('level'),
                                event.get('location'),
                                event.get('method'),
                                event.get('page'),
                                event.get('registration'),
                                event.get('sessionId'),
                                event.get('song'),
                                event.get('status'),
                                event.get('ts'),
                                event.get('userAgent'),
                                int(event.get('userId')) if event.get('userId') not in ('', None, '') else None
                            ))
                        inserted_rows += 1

            except Exception as e:
                print(f"[ERROR] processing {filepath}: {e}")

    pg_conn.commit()
    cursor.close()
    pg_conn.close()

    print(f"Loader finished: scanned {total_files} files, inserted {inserted_rows} rows.")
    if inserted_rows == 0:
        raise ValueError(f"No rows inserted into {table} — check data path or loader logic.")

