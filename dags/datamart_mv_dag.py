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
        logger.info("✅ Подключение к Greenplum прошло успешно")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к Greenplum: {e}")
        raise


@task_group(group_id="create_mart_views")
def create_mart_views():
    @task
    def lesson_popularity_summary():
        query = """
        DROP MATERIALIZED VIEW IF EXISTS mart.lesson_popularity_summary;
        CREATE MATERIALIZED VIEW mart.lesson_popularity_summary AS
        SELECT 
            DL.lesson_id,
            DL.title AS lesson_title,
            DC.course_id,
            DC.title AS course_title,
            COUNT(*) AS total_views,
            COUNT(DISTINCT FLV.user_id) AS unique_users,
            MIN(FLV.viewed_at) AS first_view,
            MAX(FLV.viewed_at) AS last_view
        FROM core.dim_lesson AS DL
        LEFT JOIN core.dim_course AS DC USING(course_id)
        LEFT JOIN core.fact_lesson_views AS FLV USING(lesson_id)
        GROUP BY 
            DL.lesson_id,
            DL.title,
            DC.course_id,
            DC.title
        DISTRIBUTED BY (lesson_id);
        """
        hook = PostgresHook(postgres_conn_id='greenplum_docker')
        hook.run(query)
        logger.info("✅ Витрина lesson_popularity_summary пересоздана")

    @task
    def inactive_users_summary():
        query = """
        DROP MATERIALIZED VIEW IF EXISTS mart.inactive_users_summary;
        CREATE MATERIALIZED VIEW mart.inactive_users_summary AS
        WITH cte_inactive_users AS (
            SELECT
                DU.user_id,
                DU.name AS user_name,
                DU.email,
                DU.age,
                DU.registration_date
            FROM core.dim_user AS DU
            LEFT JOIN core.fact_lesson_views AS FLV USING(user_id)
            WHERE FLV.user_id IS NULL
        )
        SELECT
            CIU.user_id,
            CIU.user_name,
            CIU.email,
            CIU.age,
            CIU.registration_date,
            COUNT(*) AS enrollments_count 
        FROM cte_inactive_users AS CIU
        JOIN core.fact_enrollments AS FE USING(user_id)
        GROUP BY
            CIU.user_id,
            CIU.user_name,
            CIU.email,
            CIU.age,
            CIU.registration_date
        DISTRIBUTED BY (user_id);
        """
        hook = PostgresHook(postgres_conn_id='greenplum_docker')
        hook.run(query)
        logger.info("✅ Витрина inactive_users_summary пересоздана")

    @task
    def course_completion_rate():
        query = """
        DROP MATERIALIZED VIEW IF EXISTS mart.course_completion_rate;
        CREATE MATERIALIZED VIEW mart.course_completion_rate AS 
        WITH cte_lessons AS(
            SELECT 
                course_id,
                COUNT(*) AS lessons_in_course
            FROM core.dim_lesson
            GROUP BY course_id
        )
        SELECT
            DU.user_id,
            DU.name AS user_name,    
            FE.course_id,
            DC.title AS course_title,
            CL.lessons_in_course,
            COUNT(DISTINCT FLV.lesson_id) AS lessons_viewed,        
            ROUND(COUNT(DISTINCT FLV.lesson_id)::DECIMAL / NULLIF(CL.lessons_in_course, 0), 2) AS completion_rate
        FROM core.fact_enrollments AS FE
        LEFT JOIN core.dim_user AS DU ON DU.user_id = FE.user_id
        JOIN core.dim_course AS DC ON DC.course_id = FE.course_id
        JOIN cte_lessons AS CL ON FE.course_id = CL.course_id
        LEFT JOIN core.fact_lesson_views AS FLV ON FE.user_id = FLV.user_id AND FLV.course_id = FE.course_id
        GROUP BY
            DU.user_id,
            DU.name,    
            FE.course_id,
            DC.title,
            CL.lessons_in_course
        DISTRIBUTED BY (user_id);
        """
        hook = PostgresHook(postgres_conn_id='greenplum_docker')
        hook.run(query)
        logger.info("✅ Витрина course_completion_rate пересоздана")

    # Параллельное выполнение
    lesson_popularity_summary()
    inactive_users_summary()
    course_completion_rate()


@dag(
    dag_id='datamart_mv_dag',
    start_date=datetime(2025, 9, 10),
    schedule=None,
    catchup=False,
    tags=['datamart', 'materialized_views']
)
def datamart_mv_dag():
    conn_check = check_gp_connection()
    mv_tasks = create_mart_views()
    conn_check >> mv_tasks


dag = datamart_mv_dag()
