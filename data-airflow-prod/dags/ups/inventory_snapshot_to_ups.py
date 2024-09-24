import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.ups.inventory_snapshot_to_ups import \
    send_inventory_snapshot_on_weekdays
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'send_inventory_snapshot_to_ups'
EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'limit_memory': '1G',
        'limit_cpu': '1500m'
    }
}
logger = logging.getLogger(__name__)


default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 6, 1),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=5)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='send inventory snapshot to ups',
    schedule_interval=get_schedule("0 21 * * 1-5"),
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=15),
    tags=['Automation', 'UPS']
)

send_reports = PythonOperator(
    dag=dag,
    task_id='send_reports',
    python_callable=send_inventory_snapshot_on_weekdays,
    executor_config=EXECUTOR_CONFIG
)
