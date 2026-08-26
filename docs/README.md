
# Ecology Data Pipeline

Учебный end-to-end data engineering проект по сбору, обработке и аналитике данных о качестве воздуха.

## Что реализовано

- Получение данных о качестве воздуха из внешнего API
- Автоматизированная загрузка данных с помощью Apache Airflow
- Хранение raw-данных в S3-compatible object storage (MinIO)
- Сохранение данных в формате Parquet
- Перенос данных из S3 в PostgreSQL DWH с использованием DuckDB
- Организация слоёв данных `raw → ods → stg → dm`
- Идемпотентная загрузка данных
- Настройка зависимостей между DAG'ами через `ExternalTaskSensor`
- Создание аналитической витрины `dm.air_quality_daily`
- Подключение PostgreSQL к Metabase
- Создание BI-дашборда для анализа качества воздуха
- Контейнеризация всех основных компонентов проекта с помощью Docker Compose
- Вынесение общей конфигурации и повторяющейся логики в отдельные модули

---

## Архитектура

```mermaid
flowchart TD
    API[Air Quality API]
    AF[Airflow]
    S3[MinIO / S3<br/>RAW / Parquet]
    D[DuckDB]
    PG[PostgreSQL DWH<br/>ODS → STG → DM]
    MB[Metabase<br/>BI Dashboard]

    API --> AF
    AF --> S3
    S3 --> D
    D --> PG
    PG --> MB
````

### Как проходит данные

**API → RAW**

Airflow запускает DAG `ecology_raw_api_to_s3`, получает данные из API и сохраняет их в MinIO в формате Parquet.

Пример структуры:

```text
prod/
└── raw/
    └── air_quality/
        └── YYYY-MM-DD/
            └── data.parquet
```

**RAW → ODS**

DAG `ecology_raw_s3_to_pg` забирает Parquet из MinIO и загружает данные в PostgreSQL в таблицу:

```text
ods.fct_air_quality
```

В качестве ETL-движка используется DuckDB.

**ODS → DM**

DAG `dm_air_quality_daily` рассчитывает ежедневные агрегаты и формирует аналитическую витрину:

```text
dm.air_quality_daily
```

Витрина содержит средние и максимальные значения:

* PM10
* PM2.5
* NO2

---

## Data layers

| Layer | Назначение                                   |
| ----- | -------------------------------------------- |
| `raw` | Исходные данные в S3/MinIO в формате Parquet |
| `ods` | Данные в PostgreSQL после загрузки из RAW    |
| `stg` | Временный слой для расчёта витрин            |
| `dm`  | Готовые аналитические витрины для BI         |

---

## Airflow

В проекте реализовано 3 DAG:

```text
ecology_raw_api_to_s3
        ↓
ecology_raw_s3_to_pg
        ↓
dm_air_quality_daily
```

Для связывания пайплайнов используется `ExternalTaskSensor`.

Также реализованы:

* ежедневный запуск DAG'ов;
* retries;
* контроль зависимостей;
* обработка ошибок;
* ограничение количества активных запусков.

### Airflow UI

![Airflow DAGs](screenshots/airflow_dags.png)

---

## MinIO / S3

Raw-слой хранится в MinIO как S3-compatible object storage.

![MinIO Raw Layer](screenshots/minio_raw.png)

---

## PostgreSQL DWH

Основные таблицы проекта:

```text
ods.fct_air_quality
dm.air_quality_daily
```

![PostgreSQL DWH](screenshots/postgres_dwh.png)

---

## Metabase

На основе аналитической витрины собран BI-дашборд в Metabase.

Он используется для анализа динамики показателей качества воздуха.

![Metabase Dashboard](screenshots/metabase_dashboard.png)

---

## Что я изучил и какие навыки применил

### Data Engineering

* построение ETL/ELT pipeline;
* работа с внешними API;
* оркестрация процессов в Airflow;
* работа с S3-compatible storage;
* работа с Parquet;
* использование DuckDB для ETL;
* PostgreSQL и построение DWH;
* разделение данных на `raw / ods / stg / dm`;
* идемпотентные загрузки;
* построение зависимостей между ETL-процессами.

### Data Analytics / BI

* подготовка данных для аналитики;
* создание агрегированных витрин;
* определение метрик на уровне витрины;
* подключение DWH к BI-системе;
* построение dashboard в Metabase.

### Engineering

* Docker / Docker Compose;
* переиспользование общей конфигурации;
* вынесение повторяющейся логики;
* работа с credentials и подключениями различных сервисов;
* debugging контейнеризированного окружения.

---

## Tech Stack

| Технология     | Использование                |
| -------------- | ---------------------------- |
| Python         | ETL и логика DAG             |
| Apache Airflow | Оркестрация                  |
| DuckDB         | ETL и работа с Parquet/S3    |
| MinIO          | S3-compatible object storage |
| PostgreSQL     | DWH                          |
| SQL            | Трансформации и витрины      |
| Metabase       | BI / dashboard               |
| Docker Compose | Локальная инфраструктура     |

---

## Структура проекта

```text
.
├── dags/
│   ├── ecology_api_to_s3.py
│   ├── ecology_raw_s3_to_pg.py
│   └── dm_air_quality_daily.py
├── plugins/
│   └── utils/
│       ├── dag_config.py
│       └── dates.py
├── metabase/
├── config/
├── docker-compose.yml
├── req.txt
└── LICENSE
```

---

## Запуск

Проект рассчитан на локальный запуск через Docker Compose.

### 1. Запуск контейнеров

```bash
docker compose up -d
```

После запуска доступны:

```text
Airflow    → http://localhost:8080
MinIO      → http://localhost:9001
Metabase   → http://localhost:3000
PostgreSQL → localhost:5432
```

### 2. Первичная настройка

Для работы пайплайна необходимо:

* создать bucket `prod` в MinIO;
* создать access key / secret key для S3;
* добавить необходимые значения в Airflow Variables;
* создать PostgreSQL Connection для DWH;
* подготовить PostgreSQL-пользователя / credentials для подключения к DWH;
* создать таблицу аналитической витрины `dm.air_quality_daily`.

После этого DAG'и можно запускать и отслеживать их выполнение через Airflow.

---

## Результат

В результате получен полный локальный pipeline:

```mermaid
flowchart LR
    API[Air Quality API] --> AF[Airflow]
    AF --> S3[MinIO / S3]
    S3 --> D[DuckDB]
    D --> PG[PostgreSQL DWH]
    PG --> DM[DM]
    DM --> MB[Metabase]
````


Проект демонстрирует практический опыт работы с ingestion, orchestration, object storage, DWH, SQL transformations и BI.
