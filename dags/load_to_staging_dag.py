from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task, dag, task_group
from airflow.models.baseoperator import chain
from datetime import datetime
import logging


# Фиксированный маппинг таблиц вместо динамического
TABLE_MAPPING = {
    "core.users": "staging.users",
    "core.courses": "staging.courses",
    "core.lessons": "staging.lessons",
    "core.enrollments": "staging.enrollments",
    "core.lesson_views": "staging.lesson_views"
}

logger = logging.getLogger(__name__)

@task
def check_pg_connection():
    hook = PostgresHook(postgres_conn_id='local_postgres')
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        logger.info("✅ Подключение к Postgres прошло успешно")
        cursor.close()
        conn.close()
    except Exception as e:
        raise RuntimeError(f"❌ Ошибка подключения к Postgres: {e}")


@task
def check_gp_connection():
    hook = PostgresHook(postgres_conn_id='greenplum_docker')
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        logger.info("✅ Подключение к Greenplum прошло успешно")
        cursor.close()
        conn.close()
    except Exception as e:
        raise RuntimeError(f"❌ Ошибка подключения к Greenplum: {e}")


@task_group
def transfer_table_group(source_table: str, target_table: str):
    @task
    def truncate_target(table_name: str):
        hook = PostgresHook(postgres_conn_id='greenplum_docker')
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            # Безопасное использование имен таблиц
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()
            logger.info(f"Таблица {table_name} очищена")
        finally:
            cursor.close()
            conn.close()

    @task
    def load_data(source: str, target: str):
        pg_hook = PostgresHook(postgres_conn_id='local_postgres')
        gp_hook = PostgresHook(postgres_conn_id='greenplum_docker')

        try:
            # Получаем схему таблицы
            pg_conn = pg_hook.get_conn()
            pg_cursor = pg_conn.cursor()
            pg_cursor.execute(f"SELECT * FROM {source} LIMIT 0")
            columns = [desc[0] for desc in pg_cursor.description]
            pg_cursor.close()
            pg_conn.close()

            columns_str = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))

            # Порционная выгрузка и загрузка данных
            batch_size = 1000
            offset = 0

            while True:
                pg_conn = pg_hook.get_conn()
                pg_cursor = pg_conn.cursor()
                pg_cursor.execute(f"SELECT * FROM {source} ORDER BY 1 LIMIT {batch_size} OFFSET {offset}")

                rows = pg_cursor.fetchall()
                if not rows:
                    break

                gp_conn = gp_hook.get_conn()
                gp_cursor = gp_conn.cursor()
                insert_query = f"INSERT INTO {target} ({columns_str}) VALUES ({placeholders})"

                gp_cursor.executemany(insert_query, rows)
                gp_conn.commit()

                logger.info(f"✅ Загружено {len(rows)} строк из {source} в {target} (offset: {offset})")

                gp_cursor.close()
                gp_conn.close()
                pg_cursor.close()
                pg_conn.close()

                offset += batch_size

        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке данных: {e}")
            raise

    truncate = truncate_target(table_name=target_table)
    load = load_data(source=source_table, target=target_table)
    truncate >> load


@dag(
    dag_id='load_to_staging',
    start_date=datetime(2025, 9, 9),
    schedule=None,
    catchup=False,
    tags=['load_to_staging']
)
def load_to_staging():
    pg_check = check_pg_connection()
    gp_check = check_gp_connection()

    # Создаем группы задач для каждой пары таблиц
    transfer_groups = []
    for source, target in TABLE_MAPPING.items():
        table_name = target.split('.')[-1]
        group = transfer_table_group.override(group_id=f"transfer_{table_name}")(
            source_table=source,
            target_table=target
        )
        transfer_groups.append(group)

    # Устанавливаем зависимости
    chain(pg_check, gp_check, transfer_groups)


dag = load_to_staging()