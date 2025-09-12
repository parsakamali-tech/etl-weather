from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# seting up owners and past succcess and retries number
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="weather_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily", # daily at midnight
    catchup=False, # not working automaticly for past dates 
    tags=["weather", "pyspark", "postgres"],
) as dag:

    # creating the table in postgres if not exists
    create_table = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres_default",   # setup postgres connection in airflow ui
        sql="/opt/airflow/sql/create_tables.sql"
    )

    bronze = SparkSubmitOperator(
        task_id="bronze_ingest",
        application="/opt/airflow/spark_jobs/bronze_ingest.py",
        application_args=["config/config.yaml"] # the main function argument main(**config path**)
    )

    silver = SparkSubmitOperator(
        task_id="silver_clean",
        application="/opt/airflow/spark_jobs/silver_clean.py",
    )

    gold = SparkSubmitOperator(
        task_id="gold_aggregate_and_load",
        application="/opt/airflow/spark_jobs/gold_agg_weather_daily.py",
    )

    create_table >> bronze >> silver >> gold
