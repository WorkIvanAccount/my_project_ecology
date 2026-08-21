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