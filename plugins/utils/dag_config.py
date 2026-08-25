import pendulum
from airflow.models import Variable

# --- Общее ---
OWNER = "i.skitev"

BASE_ARGS = {
    "owner": OWNER,
    "catchup": True,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

def make_args(start_date: pendulum.DateTime, **overrides) -> dict:
    """Собирает default_args для DAG'а: базовые + свой start_date + любые переопределения."""
    return {**BASE_ARGS, "start_date": start_date, **overrides}

# --- Minio / S3 ---
MINIO_ACCESS_KEY = Variable.get("minio_access_key", default_var="minioadmin")
MINIO_SECRET_KEY = Variable.get("minio_secret_key", default_var="minioadmin")
MINIO_ENDPOINT = "minio:9000"
RAW_LAYER = "raw"

# --- PostgreSQL DWH ---
PG_PASSWORD = Variable.get("pg_password", default_var="postgres")