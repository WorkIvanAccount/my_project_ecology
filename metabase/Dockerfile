# Используем официальный образ Metabase как базу
FROM metabase/metabase:v0.52.13

# Создаем директорию для плагинов
RUN mkdir -p /plugins

# Скачиваем плагин DuckDB (актуальная версия на момент 2026)
ADD https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/v0.52.13/duckdb.metabase-driver.jar /plugins/

# Устанавливаем права доступа
RUN chmod 744 /plugins/duckdb.metabase-driver.jar

# Переопределяем переменную окружения для директории плагинов
ENV MB_PLUGINS_DIR=/plugins