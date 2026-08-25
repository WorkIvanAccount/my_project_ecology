import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from utils.dates import get_data_interval_dates
from utils.dag_config import make_args, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT, RAW_LAYER

DAG_ID = "ecology_raw_api_to_s3"
LAYER = RAW_LAYER
SOURCE = "air_quality"
# ТРЕТИЙ АРГУМЕНТ ДЕНЬ - АПИ ДАЕТ ЗАДНИМ ЧИСЛОМ, ЕСЛИ НАПИСАЛ 15, ТО АПИ ВЕРНЕТ ДАННЫЕ ЗА 14
# Исправить можно в dag_config.py, но пока оставим так, чтобы не ломать текущие DAG'и (впадлу, но возможное решение предлагаю в будущем)
args = make_args(pendulum.datetime(2026, 8, 15, tz="Europe/Moscow"))

LONG_DESCRIPTION = """..."""
SHORT_DESCRIPTION = "ETL: Raw Ecology API -> Minio S3"

def load_ecology_data_to_s3(**context):
    start_date, end_date = get_data_interval_dates(**context)
    logging.info(f"🚀 Start loading data for period: {start_date} to {end_date}")

    con = duckdb.connect()
    try:
        con.sql("INSTALL httpfs; LOAD httpfs;")
        con.sql("SET s3_url_style = 'path';")
        con.sql(f"SET s3_endpoint = '{MINIO_ENDPOINT}';")
        con.sql(f"SET s3_access_key_id = '{MINIO_ACCESS_KEY}';")
        con.sql(f"SET s3_secret_access_key = '{MINIO_SECRET_KEY}';")
        con.sql("SET s3_use_ssl = FALSE;")

        api_url = (
            f"https://air-quality-api.open-meteo.com/v1/air-quality"
            f"?latitude=55.75&longitude=37.61"
            f"&start_date={start_date}&end_date={start_date}"
            f"&hourly=pm10,pm2_5,nitrogen_dioxide"
            f"&timezone=Europe%2FMoscow"
        )

        query = f"""
        COPY
        (
            SELECT
                unnest(hourly.time)             AS time,
                unnest(hourly.pm10)             AS pm10,
                unnest(hourly.pm2_5)            AS pm2_5,
                unnest(hourly.nitrogen_dioxide) AS nitrogen_dioxide
            FROM read_json_auto('{api_url}')
        )
        TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/data.parquet'
        (FORMAT PARQUET, COMPRESSION GZIP);
        """
        con.sql(query)
        logging.info(f"✅ Successfully saved data to S3 for date: {start_date}")
    except Exception as e:
        logging.error(f"❌ Error during data loading: {e}")
        raise e
    finally:
        con.close()

with DAG(
    dag_id=DAG_ID,
    schedule_interval="@daily",
    default_args=args,
    tags=["ecology", "s3", "raw"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")
    extract_load = PythonOperator(
        task_id="extract_and_load_to_s3",
        python_callable=load_ecology_data_to_s3,
    )
    end = EmptyOperator(task_id="end")

    start >> extract_load >> end