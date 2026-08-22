def get_data_interval_dates(**context) -> tuple[str, str]:
    """
    Универсальная функция для получения дат из контекста Airflow.
    """
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date