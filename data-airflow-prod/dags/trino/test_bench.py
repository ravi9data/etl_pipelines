import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.trino import test_bench
from plugins.dag_utils import get_schedule

DAG_ID = 'trino_test_bench'

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 6, 9),
    'execution_timeout': datetime.timedelta(minutes=30)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Test bench for Trino',
    schedule_interval=get_schedule('*/15 * * * *'),
    max_active_runs=2,
    catchup=False,
    tags=[DAG_ID, 'trino']
)


ml_user_session = PythonOperator(
    dag=dag,
    task_id='query_ml_user_session',
    python_callable=test_bench.runner,
    op_args=[test_bench.query_ml_user_session]
)

ml_user_session_bucketed = PythonOperator(
    dag=dag,
    task_id='query_ml_user_session_bucketed',
    python_callable=test_bench.runner,
    op_args=[test_bench.query_ml_user_session_bucketed]
)

geolocation = PythonOperator(
    dag=dag,
    task_id='query_geolocation',
    python_callable=test_bench.runner,
    op_args=[test_bench.query_geolocation]
)
