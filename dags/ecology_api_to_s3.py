import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# --- Конфигурация ---
OWNER = "i.skitev"
DAG_ID = "ecology_raw_api_to_s3"

# Параметры для S3 (Minio)
LAYER = "raw"
SOURCE = "air_quality" # Например, данные о качестве воздуха

# Получаем ключи из переменных Airflow (их нужно создать в UI)
ACCESS_KEY = Variable.get("minio_access_key", default_var="minioadmin")
SECRET_KEY = Variable.get("minio_secret_key", default_var="minioadmin")

LONG_DESCRIPTION = """
# Описание DAG
Загружает сырые данные об экологии из открытого API и сохраняет их в Minio (S3) в формате Parquet.
Используется DuckDB для эффективной обработки и загрузки.
"""

SHORT_DESCRIPTION = "ETL: Raw Ecology API -> Minio S3"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 8, 22, tz="Europe/Moscow"),
    "catchup": False, # Для тестов лучше False, чтобы не гнать историю
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

def get_dates(**context) -> tuple[str, str]:
    """Получаем даты интервала выполнения."""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date

def load_ecology_data_to_s3(**context):
    """Основная задача: запрос к API и сохранение в S3 через DuckDB."""
    
    start_date, end_date = get_dates(**context)
    logging.info(f"🚀 Start loading data for period: {start_date} to {end_date}")
    
    con = duckdb.connect()

    try:
        # Настройка расширений и подключения к S3 (Minio)
        con.sql("INSTALL httpfs; LOAD httpfs;")
        con.sql("SET s3_url_style = 'path';")
        # Важно: указываем внутренний адрес minio из docker-compose сети
        con.sql("SET s3_endpoint = 'minio:9000';") 
        con.sql(f"SET s3_access_key_id = '{ACCESS_KEY}';")
        con.sql(f"SET s3_secret_access_key = '{SECRET_KEY}';")
        con.sql("SET s3_use_ssl = FALSE;") # Для локального Minio обычно HTTP

        # Пример API: Open-Meteo (Air Quality)
        # В реальном проекте замените URL на нужный вам API
        api_url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude=55.75&longitude=37.61&start_date={start_date}&end_date={end_date}&hourly=pm10,pm2_5,nitrogen_dioxide"

        query = f"""
        COPY
        (
            SELECT * FROM read_json_auto('{api_url}')
        ) 
        TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/data.parquet' 
        (FORMAT PARQUET, COMPRESSION GZIP);
        """
        
        logging.info(f"📡 Executing DuckDB query...")
        con.sql(query)
        logging.info(f"✅ Successfully saved data to S3 for date: {start_date}")

    except Exception as e:
        logging.error(f"❌ Error during data loading: {e}")
        raise e
    finally:
        con.close()

with DAG(
    dag_id=DAG_ID,
    schedule_interval="@daily", # Или "0 5 * * *"
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