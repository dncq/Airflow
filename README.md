# TuneStream ETL Pipeline (Airflow‑Postgres)

This project implements an end-to-end ETL pipeline called **TuneStream**, built with **Apache Airflow 3.0.3**. It ingests JSON song and log data, stages it in PostgreSQL, transforms it into a star‑schema data warehouse, runs data quality checks, and orchestrates the workflow via Airflow.

---

## Project Structure

```
.
├── dags/
│   └── etl_dag.py              # Main Airflow DAG
├── plugins/
│   └── helpers/
│       └── sql_queries.py      # SQL statements for data loading
├── data/
│   ├── song_data/              # Song JSON files under nested folders
│   └── log_data/               # Log JSON files
├── create_tables.sql          # Schema creation script
├── docker-compose.yml         # Local dev setup with Airflow & Postgres
└── README.md
```

---

## Components

### Apache Airflow DAG (`etl_dag.py`)
- Orchestrates tasks using:
  - `EmptyOperator` to mark start/end
  - `PythonOperator` for JSON ingestion
  - `SQLExecuteQueryOperator` for star‑schema table inserts
- Ensures idempotency and resilience with retries, logging, and no backfill

### SQL File (`sql_queries.py` + `create_tables.sql`)
- `create_tables.sql` initializes the staging and production star‑schema tables
- `sql_queries.py` holds `INSERT … SELECT` statements to populate fact/dimension tables

### Python Loader Functions
- Recursively walk through nested folders
- Load JSON files into staging tables
- Skip invalid JSON and print warnings

### Data Quality Checks
- Ensures key tables (e.g. `songplays`, `users`) have records
- Fails fast if any check fails

### Docker Compose (`docker-compose.yml`)
- Boots:
  - PostgreSQL
  - Airflow webserver & scheduler
- Maps local folders into containerized environment

---

## Local Setup

1. **Clone the repo:**
   ```bash
   git clone <repo-url>
   cd <project>
   ```

2. **Adjust data directory**:
   Place your JSON files under `data/song_data/...` and `data/log_data/...`

3. **Start services:**
   ```bash
   docker-compose up --build
   ```
   - Airflow UI: `http://localhost:8080` (default credentials: `admin/admin`)
   - PostgreSQL: `host=localhost port=5432 db=airflow user=postgres password=postgres`

4. **Initialize Tables**:
   The database loads schema from `create_tables.sql` on startup.

5. **Inspect DAG in Airflow UI:**
   - Enable and trigger `etl_pipeline_local_postgres`
   - Monitor task execution and logs

---

## Testing & Validation

- **Directory parser test**:
  - Use `test_json_structure.py` to ensure your data is correctly discovered
- **Airflow validation**:
  ```bash
  airflow dags list
  airflow dags show etl_pipeline_local_postgres
  ```

---

## Installation Requirements

Use Python 3.10 and install with:

```bash
pip install "apache-airflow[postgres,standard]==3.0.3" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.3/constraints-3.10.txt"
pip install psycopg2-binary
```

---

## How It Works 

1. **Stage Song Data:** parse and insert into `staging_songs`
2. **Stage Log Data:** parse and insert into `staging_events`
3. **Load Fact:** build `songplays` from joined staging data
4. **Load Dimensions:** populate `users`, `songs`, `artists`, `time`
5. **Data Quality:** basic sanity checks ensure no empty datasets
6. **Completion:** mark pipeline end

---




