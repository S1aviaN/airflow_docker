from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task, dag, task_group
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@task
def check_gp_connection():
    hook = PostgresHook(postgres_conn_id='greenplum_docker')
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        logger.info("Подключение к Greenplum прошло успешно")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка подключения к Greenplum: {e}")
        raise


@task_group(group_id="refresh_mart_views")
def refresh_mart_views():
    @task
    def refresh_lesson_popularity():
        query = "REFRESH MATERIALIZED VIEW mart.lesson_popularity_summary;"
        hook = PostgresHook(postgres_conn_id='greenplum_docker')
        hook.run(query)
        logger.info("Витрина lesson_popularity_summary обновлена")

    @task
    def refresh_inactive_users():
        query = "REFRESH MATERIALIZED VIEW mart.inactive_users_summary;"
        hook = PostgresHook(postgres_conn_id='greenplum_docker')
        hook.run(query)
        logger.info("Витрина inactive_users_summary обновлена")

    @task
    def refresh_course_completion():
        query = "REFRESH MATERIALIZED VIEW mart.course_completion_rate;"
        hook = PostgresHook(postgres_conn_id='greenplum_docker')
        hook.run(query)
        logger.info("Витрина course_completion_rate обновлена")

    # Параллельное выполнение
    refresh_lesson_popularity()
    refresh_inactive_users()
    refresh_course_completion()


@dag(
    dag_id='refresh_mv_dag',
    start_date=datetime(2025, 9, 10),
    schedule=None,
    catchup=False,
    tags=['datamart', 'refresh']
)
def refresh_mv_dag():
    conn_check = check_gp_connection()
    refresh_tasks = refresh_mart_views()
    conn_check >> refresh_tasks


dag = refresh_mv_dag()
