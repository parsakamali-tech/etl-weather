# Weather ETL (PySpark + Airflow + PostgreSQL/Oracle)

End-to-end, modular ETL pipeline that ingests a weather CSV into a Data Lake (Bronze/Silver/Gold) with PySpark, orchestrated by Apache Airflow, and publishes daily aggregates to PostgreSQL (default) or Oracle (optional).

---

## Architecture

- **Bronze (raw)**: CSV → Parquet, partitioned by year/month/day, normalized column names, timestamp parsed.
- **Silver (clean)**: Typed columns, trimming text, basic DQ flag (dq_missing_required), event_date/event_hour.
- **Gold (serving)**: Daily aggregates (avg/min/max temperature, avg humidity, pressure, wind speed).
- **Sink**: Write Gold daily to PostgreSQL (default) or Oracle via JDBC.
- **Orchestration**: Airflow DAG → bronze_ingest → silver_clean → gold_aggregate_and_load.

---

## Tech stack

- PySpark 3.5.x (local mode or Spark Standalone)
- Apache Airflow 2.10.0 (recommended)
- PostgreSQL 15

---

## Project Structure

```
etl-weather/
├─ config/
│  └─ config.yaml
├─ data/
├─ dags/
│  └─ weather_etl_dag.py
├─ spark_jobs/
│  ├─ common.py
│  ├─ bronze_ingest.py
│  ├─ silver_clean.py
│  └─ gold_agg_weather_daily.py
├─ sql/
│  └─ create_tables.sql
├─ docker-compose.yml
├─ requirements.txt
└─ README.md
        
```

---


---

## Configuration

Edit **`config/config.yaml`**:

```yaml
lake:
  base_path: "file:///opt/data-lake"
  bronze:    "file:///opt/data-lake/bronze/weather"
  silver:    "file:///opt/data-lake/silver/weather"
  gold:      "file:///opt/data-lake/gold/weather"

input:
  csv_path: "file:///opt/input/weather.csv"

spark:
  app_name: "weather-etl"
  master: "local[*]"          # or spark://spark-master:7077
  shuffle_partitions: 4

# Choose your sink (Postgres by default)
sink:
  # PostgreSQL
  url: "jdbc:postgresql://postgres:5432/datalake"
  driver: "org.postgresql.Driver"
  user: "etl_user"
  password: "etl_password"
  table_daily: "public.weather_daily"

  # Oracle (uncomment to use)
  # url: "jdbc:oracle:thin:@//oracle-db:1521/ORCLPDB1"
  # driver: "oracle.jdbc.OracleDriver"
  # user: "YOUR_SCHEMA_USER"
  # password: "YOUR_SCHEMA_PASS"
  # table_daily: "YOURSCHEMA.WEATHER_DAILY"

dq:
  required_columns: ["formatted_date","temperature_c","humidity"]

```

---

## Data

i locally upload the raw data in "file:///opt/input/weather.csv".but i use airflow anyway to show my skills wich i recently learn in it.

1) Bronze
    - Normalizing column names
    - Transforming date columns to timestamp
2) Silver
    - The desire type of columns(numbers columns transformed into double type)
    - Deleting " " & "\n" & "\t" & "\r" from strings
    - Checking if our requirments column(in config file at dq) are not droped
    - Extracting date and hour as a independ column
    - Droping the times that has no date\
3) Gold
    - Geting the min & avg & sum & max of temprature, humidity and pressure

---

## Quick start

1) Prereqs

- Docker + Docker Compose
- Put data/weather.csv locally; it is mounted into the Airflow container at /opt/input/weather.csv
- Bring up the service ``` bash docker-compose up -d ```
- downloading package apache-airflow-providers-apache-spark==5.3.2 ``` bash docker exec 53e0c16c386ce9140b9c22cf1c729cb9396d6d9534eb5ab899326dc04082b4ec python -m pip install apache-airflow-providers-apache-spark==5.3.2 ```

2) Airflow connection (only if you use PostgresOperator for DDL)

Airflow UI → Admin → Connections:

 - Conn ID: postgres_default

 - Conn Type: Postgres

 - Host: postgres, Port: 5432

 - Schema: datalake

 - Login: etl_user, Password: etl_password

3) Trigger the DAG

Open http://localhost:8080, turn on weather_etl, and trigger a run.

---

## Data Quality & next steps

- Add Great Expectations / Soda checks in the Silver step.

- Switch lake.base_path to s3a://... for S3/MinIO (configure Hadoop/S3 credentials for Spark).

- Add dashboards (Superset/Metabase) on top of Postgres.

- Parameterize environments (dev/stage/prod) using separate config-<env>.yaml files.

---

## Author

**Parsa Kamali Shahry**
Aspiring Data Engineer
GitHub: https://github.com/parsakamali-tech
Email: parsakamlibsns@outlook.com
LinkedIn: https://www.linkedin.com/in/parsa-kamali-243934305/