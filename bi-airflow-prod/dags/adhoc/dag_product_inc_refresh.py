import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.product_inc_refresh.product_inc_refresh import run_upsert
from plugins.dag_utils import get_schedule, on_failure_callback

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 8, 11),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=5)}

DAG_ID = 'product_inc_refresh'

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=get_schedule('*/20 * * * *'),
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=15),
    tags=['redshift'])

run_upsert_from_rds = PythonOperator(
    dag=dag,
    task_id='run_upsert_from_rds',
    python_callable=run_upsert
)
