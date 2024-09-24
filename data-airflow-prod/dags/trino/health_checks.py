import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.trino import health_checks
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'trino_health_checks'


logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 7, 7),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=5),
    'on_failure_callback': on_failure_callback
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='First iteration of trino health checks',
    schedule_interval=get_schedule('*/5 * * * *'),
    catchup=False,
    max_active_runs=1,
    tags=[DAG_ID, 'trino', 'monitoring']
)


health_check_ml_userid_session_bucketed = PythonOperator(
    dag=dag,
    task_id='health_check_ml_userid_session_bucketed',
    execution_timeout=datetime.timedelta(seconds=120),
    python_callable=health_checks.runner,
    op_args=[health_checks.query_ml_userid_session_id_matching_bucketed]
)

health_check_geolocation = PythonOperator(
    dag=dag,
    task_id='health_check_geolocation',
    execution_timeout=datetime.timedelta(seconds=180),
    python_callable=health_checks.runner,
    op_args=[health_checks.query_geolocation]
)

# TODO add other relevant queries for important datasets as indicator of healthiness
