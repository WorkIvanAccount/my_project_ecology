import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from utils.dates import get_data_interval_dates

# --- Конфигурация DAG ---
OWNER = "i.skitev"
DAG_ID = "ecology_raw_s3_to_pg"

# Используемые таблицы и слои
LAYER = "raw"
SOURCE = "air_quality"
SCHEMA = "ods"
TARGET_TABLE = "fct_air_quality"

# S3 (Minio)
ACCESS_KEY = Variable.get("minio_access_key", default_var="minioadmin")
SECRET_KEY = Variable.get("minio_secret_key", default_var="minioadmin")

# PostgreSQL DWH
PG_PASSWORD = Variable.get("pg_password", default_var="postgres")

LONG_DESCRIPTION = """
# ETL: Raw S3 -> ODS PostgreSQL
Берёт сырые parquet-файлы из Minio S3 (слой raw) 
и перекладывает их в PostgreSQL DWH (слой ODS).
Используется DuckDB как ETL-движок.
"""

SHORT_DESCRIPTION = "ETL: Raw S3 (parquet) -> ODS PostgreSQL"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 8, 22, tz="Europe/Moscow"),
    "catchup": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}


def get_and_transfer_raw_data_to_ods_pg(**context):
    """Читает parquet из S3 и загружает в PostgreSQL DWH."""
    start_date, end_date = get_data_interval_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date} to {end_date}")
    
    con = duckdb.connect()

    try:
        con.sql(
            f"""
            -- Настройки для работы с S3 (Minio)
            SET TIMEZONE='UTC';
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{ACCESS_KEY}';
            SET s3_secret_access_key = '{SECRET_KEY}';
            SET s3_use_ssl = FALSE;

            -- Создаем секрет для подключения к PostgreSQL DWH
            CREATE OR REPLACE SECRET dwh_postgres (
                TYPE postgres,
                HOST 'postgres_dwh',
                PORT 5432,
                DATABASE 'dwh',
                USER 'postgres',
                PASSWORD '{PG_PASSWORD}'
            );

            -- Прикрепляем Postgres как внешнюю базу к DuckDB
            ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);

            -- DDL: Создаем схему и таблицу, если их еще нет
            CREATE SCHEMA IF NOT EXISTS dwh_postgres_db.{SCHEMA};
            
            CREATE TABLE IF NOT EXISTS dwh_postgres_db.{SCHEMA}.{TARGET_TABLE} (
                time TIMESTAMP,
                pm10 DOUBLE PRECISION,
                pm2_5 DOUBLE PRECISION,
                nitrogen_dioxide DOUBLE PRECISION
            );

            -- DML (ИДЕМПОТЕНТНОСТЬ): Удаляем данные за этот день перед вставкой
            DELETE FROM dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            WHERE time >= '{start_date} 00:00:00'::TIMESTAMP 
                AND time < '{end_date} 00:00:00'::TIMESTAMP;

            -- DML: Читаем из S3 и пишем в Postgres
            INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            (
                time,
                pm10,
                pm2_5,
                nitrogen_dioxide
            )
            SELECT
                time::TIMESTAMP,
                pm10::DOUBLE,
                pm2_5::DOUBLE,
                nitrogen_dioxide::DOUBLE
            FROM read_parquet('s3://prod/{LAYER}/{SOURCE}/{start_date}/data.parquet');
            """
        )
        logging.info(f"✅ Successfully loaded data to Postgres for date: {start_date}")

    except Exception as e:
        logging.error(f"❌ Error during data transfer: {e}")
        raise e
    finally:
        con.close()


# ВНИМАНИЕ: Этот блок должен быть в самом низу файла, БЕЗ отступов слева!
with DAG(
    dag_id=DAG_ID,
    schedule_interval="@daily",
    default_args=args,
    tags=["ecology", "s3", "ods", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="ecology_raw_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=86400,
        poke_interval=60,
    )

    transfer_to_ods = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(task_id="end")

    start >> sensor_on_raw_layer >> transfer_to_ods >> end