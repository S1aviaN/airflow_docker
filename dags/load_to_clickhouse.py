from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from clickhouse_driver import Client
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

@task
def check_clickhouse_connection():
    try:
        # Получаем параметры подключения из Airflow Connection
        conn = BaseHook.get_connection("clickhouse_default")
        client = Client(
            host=conn.host,
            port=conn.port or 9000,
            database=conn.schema,
            user=conn.login,
            password=conn.password
        )

        # Выполняем простой запрос
        result = client.execute("SELECT 1")
        logger.info(f"Подключение к ClickHouse успешно: {result}")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к ClickHouse: {e}")
        raise

@dag(
    dag_id="check_clickhouse_connection_dag",
    start_date=datetime(2025, 9, 10),
    schedule=None,
    catchup=False,
    tags=["clickhouse", "connectivity"]
)
def check_clickhouse_connection_dag():
    check_clickhouse_connection()

dag = check_clickhouse_connection_dag()
