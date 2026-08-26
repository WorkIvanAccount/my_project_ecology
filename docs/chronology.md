# my_project_ecology
my_project_ecology

## Создать и активировать питон окружение
```bash
python3 -m venv venv
source venv/bin/activate
```
## Создаем конфиг докер-композ
Добавил шаблон-код с сайта аирфлоу, вписал свои нужные сервисы (postgreSQL DWH, minio S3, metabase BI).
Создал диро metabase и сделал докерфайл для будущего использования DuckDB.

### Используется для одновременного создания, сборки и запуска всех контейнеров. С ключом -d (docker-compose up -d): Контейнеры запускаются в фоне.
```bash
docker-compose up -d
```

### Были проблемы из-за прав доступа аирфлоу, создал .env и написал
```AIRFLOW_UID=50000```

### Для того чтобы создать файл в папке даг, пришлось изменять пермишон
```sudo chown -R $USER:$USER /home/ivanskitev/CatProject/Project/my_project_ecology/dags```

### Чтобы доустановить duckdb и pendulum в контейнеры Airflow, добавил переменную окружения в каждый airflow-сервис (webserver, scheduler, worker, triggerer). Важно: первая строка сливает общий env — без неё сервис теряет коннект к БД и падает с "You need to initialize the database".
```yaml
    environment:
      <<: *airflow-common-env
      AIRFLOW_UID: 50000
      _PIP_ADDITIONAL_REQUIREMENTS: duckdb pendulum
```

### Airflow не видел Minio по имени `minio` (Could not resolve hostname): сервисы сидели в разных docker-сетях. Сделал единую сеть дефолтной для всего композа:
```yaml
networks:
  default:
    name: eco-network
    driver: bridge
```
```bash
docker compose down --remove-orphans
docker network prune
docker compose up -d
# проверка, что все в одной сети
docker network inspect eco-network --format '{{range .Containers}}{{.Name}} {{end}}'
```

### Minio не создает бакеты сам. Создал бакет `prod` через консоль http://localhost:9001 (minioadmin/minioadmin).
Опционально: одноразовый контейнер minio-init с `mc mb`, чтобы бакет создавался сам при каждом старте.

### Написал первый DAG ecology_api_to_s3.py (папка dags/)
DuckDB + httpfs качает JSON из Open-Meteo API (качество воздуха) и пишет parquet напрямую в S3, без промежуточных файлов.
Файл ложится по пути: `prod/raw/air_quality/YYYY-MM-DD/data.parquet`.
Ключи от S3 берутся из Airflow Variables (Admin -> Variables), с fallback через default_var.

### Запуск и отладка DAG
Запуск кнопкой Trigger (▶) в UI. Статусы и логи: Grid -> клик по задаче -> Log.
Первый прогон завершился SUCCESS, файл появился в Minio (Object Browser -> prod).

### Вынес общий код в плагины (DRY)
Функция получения дат дублировалась. Вынес в `plugins/utils/dates.py`.
Чтобы Airflow в докере видел плагины, добавил в `docker-compose.yml`:
```yaml
    environment:
      PYTHONPATH: /opt/airflow/plugins
```
Чтобы VS Code не подчеркивал импорты красным, создал `.vscode/settings.json`:
```json
{
    "python.analysis.extraPaths": ["./plugins"]
}
```

### Исправил первый даг, добавил unnest для нормального приведения типов в S3
```unnest(hourly.time) AS time```

```markdown
### Написал второй DAG ecology_raw_s3_to_pg.py (папка dags/)
Забирает parquet из S3 и перекладывает в PostgreSQL DWH (слой ODS).
Использует DuckDB как ETL-движок - подключается к S3 и Postgres одновременно через `ATTACH`:
```sql
CREATE OR REPLACE SECRET dwh_postgres (TYPE postgres, HOST 'postgres_dwh', ...);
ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);

INSERT INTO dwh_postgres_db.ods.fct_air_quality
SELECT time::TIMESTAMP, pm10::DOUBLE, pm2_5::DOUBLE, nitrogen_dioxide::DOUBLE
FROM read_parquet('s3://prod/raw/air_quality/{start_date}/data.parquet');
```

### Идемпотентность через DELETE/INSERT
Перед вставкой данных за день удаляем старые записи за этот же день:
```sql
DELETE FROM dwh_postgres_db.ods.fct_air_quality
WHERE time >= '{start_date} 00:00:00'::TIMESTAMP 
  AND time < '{end_date} 00:00:00'::TIMESTAMP;
```
Это гарантирует, что при повторном запуске DAG'а (Clear -> Run) не будет дублей.

### ExternalTaskSensor для связки DAG'ов
Второй DAG ждет успешного завершения первого через `ExternalTaskSensor`:
```python
sensor_on_raw_layer = ExternalTaskSensor(
    task_id="sensor_on_raw_layer",
    external_dag_id="ecology_raw_api_to_s3",
    allowed_states=["success"],
    mode="reschedule",
    timeout=86400,
    poke_interval=60,
)
```
Сенсор проверяет статус первого DAG'а каждую минуту и не занимает воркер впустую (`mode="reschedule"`).

### Результат: данные в PostgreSQL DWH
Данные успешно перелиты в таблицу `ods.fct_air_quality` (48 строк за 2 дня, почасовая гранулярность):
```sql
SELECT * FROM ods.fct_air_quality ORDER BY time;
```

### Создаем витрину
```sql
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS dm.air_quality_daily (
    date DATE PRIMARY KEY,
    avg_pm10 DOUBLE PRECISION,
    max_pm10 DOUBLE PRECISION,
    avg_pm2_5 DOUBLE PRECISION,
    max_pm2_5 DOUBLE PRECISION,
    avg_no2 DOUBLE PRECISION,
    max_no2 DOUBLE PRECISION
);
```


### Слои и схемы в DWH
- **raw** — не в PG, а в S3: сырые parquet, golden copy.
- **ods** — оперативный слой: raw с приведёнными типами (1 строка = 1 час). `ods.fct_air_quality`.
- **stg** — временные таблицы `tmp_<витрина>_<дата>` для расчёта витрин; при ошибке витрина не тронута.
- **dm** — витрины: дневные агрегаты для BI (1 строка = 1 день). `dm.air_quality_daily`.
- **public** — дефолтная схема PG, не используется. Сюда попали метаданные Metabase — вынесены в отдельную БД `metabase` (`CREATE DATABASE` + `MB_DB_DBNAME: metabase`).

### Витрина (третий DAG dm_air_quality_daily)
- Без Python/DuckDB: только `SQLExecuteQueryOperator`, SQL летит прямо в Postgres.
- Подключение через Connection `postgres_dwh` (Admin -> Connections), а не Variables.
- Паттерн STG (идемпотентность): drop tmp → создать tmp с агрегатами → delete день из витрины → insert из tmp → drop tmp.
- Даты через Jinja: `{{ data_interval_start.format('YYYY-MM-DD') }}` (в f-строке — четыре скобки `{{{{ }}}}`).
- Сенсор ждёт `ecology_raw_s3_to_pg`: данные должны быть уже в PG, а не только в S3.
- Таблицу витрины создал руками — DDL отдельно от DML.

### Дебаг: relation "ods.fct_air_quality" does not exist
Connection смотрел не в ту базу. На одном Postgres-сервере несколько баз, схемы живут внутри конкретной базы. Лечится проверкой поля Database в Connection и `SELECT current_database();`.

### Вынес общие default_args и креды в один файл (DRY, вторая волна)
`start_date`/`owner`/`retries` и ключи Minio/Postgres дублировались уже в трёх DAG'ах. 
Вынес в `plugins/utils/dag_config.py`:
```python
BASE_ARGS = {"owner": OWNER, "catchup": True, "retries": 2, "retry_delay": ...}

def make_args(start_date, **overrides):
    return {**BASE_ARGS, "start_date": start_date, **overrides}
```
Каждый DAG теперь передаёт только свою `start_date`, остальное — из общего конфига.
`MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `PG_PASSWORD` тоже туда переехали.

### Баг: за число N в БД прилетают данные N-1 (открыт, не закрыт)
Гипотеза 1 (API отдаёт `hourly.time` в UTC вместо МСК) — добавил `&timezone=Europe%2FMoscow` 
в `api_url`, проблему не решило.
Пробовал лечить сдвигом `start_date` DAG'а в `dag_config` (16→15, 22→15) — это НЕ фикс: 
`args["start_date"]` влияет только на нижнюю границу catchup, а не на дату, с которой 
реально формируется `api_url` внутри таска (та берётся из `get_data_interval_dates(**context)` 
на каждый ран отдельно).
Рабочая гипотеза 2: смещение может быть на уровне семантики Airflow 
(`data_interval_start` для `@daily` рана, выполненного физически в день X, 
логически равен X-1) — ещё не подтверждено, нужно свериться с `utils/dates.py`.

### Построен дашбоард в метабейз