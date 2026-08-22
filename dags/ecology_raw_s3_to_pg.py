import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from utils.dates import get_data_interval_dates

# КОНФИГУРАЦИЯ DAG
OWNER = "i.skitev"
DAG_ID = "ecology_raw_s3_to_pg"  # имя файла и dag_id обычно совпадают

# СЛОИ И ТАБЛИЦЫ
LAYER = "raw"
SOURCE = "air_quality"
SCHEMA = "ods"
TARGET_TABLE = "fct_air_quality"  # fct_ = факты, так принято в DWH

# КЛЮЧИ (берем из Airflow Variables, чтобы не светить в коде)
ACCESS_KEY = Variable.get("minio_access_key", default_var="minioadmin")
SECRET_KEY = Variable.get("minio_secret_key", default_var="minioadmin")
PG_PASSWORD = Variable.get("pg_password", default_var="postgres")

# ОПИСАНИЕ
LONG_DESCRIPTION = """
# ecology_raw_s3_to_pg
Берёт сырые parquet-файлы из Minio S3 (слой raw) 
и перекладывает их в PostgreSQL DWH (слой ODS).
Используется DuckDB как ETL-движок.
"""

SHORT_DESCRIPTION = "ETL: Raw S3 (parquet) -> ODS PostgreSQL"

# АРГУМЕНТЫ ПО УМОЛЧАНИЮ
args = {
    "owner": OWNER,
    # start_date берем такой же, как в первом DAGе, чтобы сенсоры корректно работали
    "start_date": pendulum.datetime(2026, 8, 22, tz="Europe/Moscow"),
    "catchup": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

def get_and_transfer_raw_data_to_ods_pg(**context):
    """
    Основная задача: читает Parquet из Minio S3 и пишет в PostgreSQL DWH.
    Использует DuckDB как ETL-движок для прямого перекладывания данных.
    """
    # Получаем даты из контекста (идемпотентность!)
    start_date, end_date = get_data_interval_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date} to {end_date}")
    
    # Инициализируем in-memory базу DuckDB
    con = duckdb.connect()

    try:
        # Выполняем весь ETL процесс одним SQL-запросом
        con.sql(
            f"""
            -- Настройки для работы с S3 (Minio)
            SET TIMEZONE='UTC';
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000'; -- Имя сервиса из docker-compose.yml
            SET s3_access_key_id = '{ACCESS_KEY}';
            SET s3_secret_access_key = '{SECRET_KEY}';
            SET s3_use_ssl = FALSE;

            -- Создаем секрет для подключения к PostgreSQL DWH
            -- HOST берем из docker-compose.yml (postgres_dwh)
            -- DATABASE берем из docker-compose.yml (dwh)
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

            -- DDL: Создаем схему и таблицу, если их еще нет (защита от первого запуска)
            CREATE SCHEMA IF NOT EXISTS dwh_postgres_db.{SCHEMA};
            
            CREATE TABLE IF NOT EXISTS dwh_postgres_db.{SCHEMA}.{TARGET_TABLE} (
                time TIMESTAMP,             -- Приводим к правильному типу, как советовал автор гайда
                pm10 DOUBLE PRECISION,
                pm2_5 DOUBLE PRECISION,
                nitrogen_dioxide DOUBLE PRECISION
            );

            -- DML (ИДЕМПОТЕНТНОСТЬ): Удаляем данные за этот день перед вставкой.
            -- Если DAG упадет и мы запустим его повторно (Clear -> Run), дублей не будет.
            DELETE FROM dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            WHERE time >= '{start_date} 00:00:00'::TIMESTAMP 
                AND time < '{end_date} 00:00:00'::TIMESTAMP;

            -- DML: Читаем из S3 и пишем в Postgres
            -- ВАЖНО: Здесь мы предполагаем, что в parquet файле колонки называются именно так.
            -- Если Open-Meteo сохранил их иначе (например, hourly.time), здесь нужно будет поправить SELECT.
            INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            (
                time,
                pm10,
                pm2_5,
                nitrogen_dioxide
            )
            SELECT
                time::TIMESTAMP,            -- Явно кастуем к Timestamp
                pm10::DOUBLE,
                pm2_5::DOUBLE,
                nitrogen_dioxide::DOUBLE
            FROM read_parquet('s3://prod/{LAYER}/{SOURCE}/{start_date}/data.parquet');
            """
        )
        logging.info(f"✅ Successfully loaded data to Postgres for date: {start_date}")

    except Exception as e:
        logging.error(f"❌ Error during data transfer: {e}")
        raise e # Пробрасываем ошибку, чтобы Airflow пометил таску как Failed и запустил retries
    finally:
        con.close() # Всегда закрываем соединение, даже при ошибке