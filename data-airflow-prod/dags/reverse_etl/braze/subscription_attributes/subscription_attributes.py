import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from business_logic.reverse_etl.braze.subscription_attributes.subscription_attributes import (
    export_redshift_data_to_s3, get_delta_records, send_records_to_braze)
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'braze_subscription_attributes_reverse_etl'
REDSHIFT_CONN = 'redshift'
EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '500M',
        'request_cpu': '500m',
        'limit_memory': '2G',
        'limit_cpu': '1500m'
    }
}

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 4, 20),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=20)
}


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='subscription attributes data to the lake',
    schedule_interval=get_schedule('15 3 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=['braze', 'reverse_etl', 'api', 'export', 'redshift', DAG_ID]
)

export_redshift_data_to_s3 = ShortCircuitOperator(
    dag=dag,
    task_id='export_redshift_data_to_s3',
    python_callable=export_redshift_data_to_s3,
    executor_config=EXECUTOR_CONFIG
)

get_delta_records = ShortCircuitOperator(
    dag=dag,
    task_id='get_delta_records',
    python_callable=get_delta_records,
    executor_config=EXECUTOR_CONFIG
)

send_records_to_braze = PythonOperator(
    dag=dag,
    task_id='send_records_to_braze',
    python_callable=send_records_to_braze,
    executor_config=EXECUTOR_CONFIG
)

export_redshift_data_to_s3 >> get_delta_records >> send_records_to_braze
