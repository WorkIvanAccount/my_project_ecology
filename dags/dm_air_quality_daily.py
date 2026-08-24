import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Конфигурация DAG
OWNER = "i.skitev"
DAG_ID = "dm_air_quality_daily"

# Используемые таблицы в DAG
SCHEMA = "dm"
TARGET_TABLE = "air_quality_daily"
ODS_TABLE = "ods.fct_air_quality"

# DWH Connection (создается в Airflow UI -> Admin -> Connections)
PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
# Витрина: Ежедневные агрегаты качества воздуха
Считает средние и максимальные значения PM10, PM2.5 и NO2 за сутки 
на основе данных из ODS слоя.
"""
SHORT_DESCRIPTION = "DM: Daily Air Quality Aggregates"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 8, 22, tz="Europe/Moscow"),
    "catchup": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    schedule_interval="@daily",
    default_args=args,
    tags=["dm", "pg", "ecology"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    # Ждем, пока второй даг успешно перельет данные в ODS (Postgres)
    sensor_on_ods_layer = ExternalTaskSensor(
        task_id="sensor_on_ods_layer",
        external_dag_id="ecology_raw_s3_to_pg", # <-- Имя второго дага!
        allowed_states=["success"],
        mode="reschedule",
        timeout=86400,
        poke_interval=60,
    )

    # 1. Удаляем временную таблицу, если она вдруг осталась с прошлого неудачного запуска
    drop_stg_table_before = SQLExecuteQueryOperator(
        task_id="drop_stg_table_before",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}";
        """,
    )

    # 2. Создаем временную таблицу и считаем агрегаты за конкретный день
    create_stg_table = SQLExecuteQueryOperator(
        task_id="create_stg_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE TABLE stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}" AS
        SELECT
            time::date AS date,
            ROUND(AVG(pm10)::numeric, 2) AS avg_pm10,
            MAX(pm10) AS max_pm10,
            ROUND(AVG(pm2_5)::numeric, 2) AS avg_pm2_5,
            MAX(pm2_5) AS max_pm2_5,
            ROUND(AVG(nitrogen_dioxide)::numeric, 2) AS avg_no2,
            MAX(nitrogen_dioxide) AS max_no2
        FROM
            {ODS_TABLE}
        WHERE
            time::date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
        GROUP BY 1;
        """,
    )

    # 3. Удаляем старые данные за этот день из финальной витрины (Идемпотентность)
    delete_from_target_table = SQLExecuteQueryOperator(
        task_id="delete_from_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DELETE FROM {SCHEMA}.{TARGET_TABLE}
        WHERE date IN (
            SELECT date FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        );
        """,
    )

    # 4. Вставляем посчитанные данные в финальную витрину
    insert_into_target_table = SQLExecuteQueryOperator(
        task_id="insert_into_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE}
        SELECT * FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}";
        """,
    )

    # 5. Убираем за собой временную таблицу
    drop_stg_table_after = SQLExecuteQueryOperator(
        task_id="drop_stg_table_after",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}";
        """,
    )

    end = EmptyOperator(task_id="end")

    (
        start 
        >> sensor_on_ods_layer 
        >> drop_stg_table_before 
        >> create_stg_table 
        >> delete_from_target_table 
        >> insert_into_target_table 
        >> drop_stg_table_after 
        >> end
    )