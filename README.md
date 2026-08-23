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