import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from business_logic.catman.pricing_availability_ideal_state import \
    get_data_from_spreadsheet
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'pricing_availability_ideal_state'
REDSHIFT_CONN = 'redshift'
EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '2G',
        'request_cpu': '1000m',
        'limit_memory': '4G',
        'limit_cpu': '2000m'
    }
}

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 3, 12),
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=120),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=15)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Extract Pricing Googlesheet data and load to Redshift',
    schedule_interval=get_schedule('0 11,16 * * 1,2,3,4,5'),
    max_active_runs=1,
    catchup=False,
    tags=['catman', 'external', 'googlesheet', 'redshift']
)

get_data_from_spreadsheet = PythonOperator(
        dag=dag,
        task_id='get_data_from_spreadsheet',
        python_callable=get_data_from_spreadsheet,
        executor_config=EXECUTOR_CONFIG)

copy_pricing_data = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="copy_pricing_data",
    sql="./sql/copy_pricing_data.sql",
    autocommit=False
)

get_data_from_spreadsheet >> copy_pricing_data
