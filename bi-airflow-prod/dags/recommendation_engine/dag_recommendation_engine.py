import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from dags.recommendation_engine.recommandation_terminal import \
    indexRecommendationsToCatalog
from plugins.dag_utils import get_schedule, on_failure_callback

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 8, 31),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback}

DAG_ID = 'recommendation_engine'

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=get_schedule('30 8,20 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=['recommendation_engine'])

run_recommendation_engine_sql = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="run_recommendation_engine_sql",
    sql="./sql/re_refresh.sql"
)

consideration_recommendation_sql = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="consideration_recommendation",
    sql="./sql/recommendation.sql"
)

index_task = PythonOperator(
        dag=dag,
        task_id='indexRecommendationsToCatalog',
        python_callable=indexRecommendationsToCatalog
    )
run_recommendation_engine_sql >> consideration_recommendation_sql >> index_task
