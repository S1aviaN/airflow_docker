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
        error_msg = f"Ошибка подключения к Greenplum: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)


@task_group(group_id="transform_dimensions")
def transform_dimensions():
    @task
    def dim_user():
        try:
            query = """
            INSERT INTO core.dim_user(user_id, "name", age, email, registration_date)
            SELECT 
                user_id::INTEGER, 
                "name"::TEXT, 
                age::INTEGER, 
                email::TEXT, 
                registration_date::DATE
            FROM staging.users;
            """
            hook = PostgresHook(postgres_conn_id='greenplum_docker')
            hook.run(query)
            logger.info("Таблица dim_user загружена")
        except Exception as e:
            logger.error(f"Ошибка загрузки таблицы dim_user: {e}")
            raise

    @task
    def dim_course():
        try:
            query = """
            INSERT INTO core.dim_course(course_id, title, category)
            SELECT 
                course_id::INTEGER, 
                title::TEXT, 
                category::TEXT
            FROM staging.courses;
            """
            hook = PostgresHook(postgres_conn_id='greenplum_docker')
            hook.run(query)
            logger.info("Таблица dim_course загружена")
        except Exception as e:
            logger.error(f"Ошибка загрузки таблицы dim_course: {e}")
            raise

    @task
    def dim_lesson():
        try:
            query = """
            INSERT INTO core.dim_lesson(lesson_id, course_id, title, duration_min)
            SELECT 
                lesson_id::INTEGER, 
                course_id::INTEGER, 
                title::TEXT, 
                duration_min::INTEGER
            FROM staging.lessons;
            """
            hook = PostgresHook(postgres_conn_id='greenplum_docker')
            hook.run(query)
            logger.info("Таблица dim_lesson загружена")
        except Exception as e:
            logger.error(f"Ошибка загрузки таблицы dim_lesson: {e}")
            raise

    dim_user()
    dim_course()
    dim_lesson()


@task_group(group_id="transform_facts")
def transform_facts():
    @task
    def fact_lesson_views():
        try:
            query = """
            INSERT INTO core.fact_lesson_views(user_id, lesson_id, course_id, viewed_at)
            SELECT 
                LV.user_id::INTEGER, 
                LV.lesson_id::INTEGER, 
                DL.course_id::INTEGER, 
                LV.viewed_at::TIMESTAMP
            FROM staging.lesson_views AS LV
            JOIN core.dim_lesson AS DL ON LV.lesson_id = DL.lesson_id;
            """
            hook = PostgresHook(postgres_conn_id='greenplum_docker')
            hook.run(query)
            logger.info("Таблица fact_lesson_views загружена")
        except Exception as e:
            logger.error(f"Ошибка загрузки таблицы fact_lesson_views: {e}")
            raise

    @task
    def fact_enrollments():
        try:
            query = """
            INSERT INTO core.fact_enrollments(user_id, course_id, enrolled_at)
            SELECT 
                user_id::INTEGER, 
                course_id::INTEGER, 
                enrolled_at::DATE
            FROM staging.enrollments;
            """
            hook = PostgresHook(postgres_conn_id='greenplum_docker')
            hook.run(query)
            logger.info("Таблица fact_enrollments загружена")
        except Exception as e:
            logger.error(f"Ошибка загрузки таблицы fact_enrollments: {e}")
            raise

    fact_lesson_views()
    fact_enrollments()


@dag(
    dag_id='dwh_transform',
    start_date=datetime(2025, 9, 9),
    schedule=None,
    catchup=False,
    tags=['load_to_core']
)
def transform_staging_to_core():
    conn_check = check_gp_connection()
    dim_tasks = transform_dimensions()
    fact_tasks = transform_facts()

    # Устанавливаем зависимости
    conn_check >> dim_tasks >> fact_tasks


dag = transform_staging_to_core()