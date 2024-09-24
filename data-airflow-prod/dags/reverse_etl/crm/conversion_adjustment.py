import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.reverse_etl.crm.conversion_adjustment import \
    load_redshift_data_to_gsheet
from dags.reverse_etl.crm.config.conversion_adjustment_config import config
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'crm_conversion_adjustment'
REDSHIFT_CONN = 'redshift'
EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '500M',
        'request_cpu': '250m',
        'limit_memory': '1G',
        'limit_cpu': '500m'
    }}

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 3, 19),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=60),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=15)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Extract CRM data from Redshift and load to Googlesheet',
    schedule_interval=get_schedule('0 2 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=['crm', 'external', 'googlesheet', 'redshift', DAG_ID]
)

for conf in config:
    conversion_adjustment_load = PythonOperator(
        dag=dag,
        task_id=f'extract_load_{conf.get("task_name")}',
        python_callable=load_redshift_data_to_gsheet,
        op_kwargs=conf,
        executor_config=EXECUTOR_CONFIG
    )
