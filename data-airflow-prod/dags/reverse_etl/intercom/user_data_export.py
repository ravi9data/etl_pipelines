import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.reverse_etl.intercom.user_data_export import (
    export_redshift_data_to_s3, send_records_to_intercom)
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'intercom_redshift_user_data_export'
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
    'start_date': datetime.datetime(2022, 3, 27),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=300),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=60)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Export user data from Redshift and send to Intercom',
    schedule_interval=get_schedule('0 0,6,12,18 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=['intercom', 'reverse_etl', 'api', 'export', 'redshift', DAG_ID]
)

export_redshift_data_to_s3 = PythonOperator(
        dag=dag,
        task_id='export_redshift_data_to_s3',
        python_callable=export_redshift_data_to_s3,
        executor_config=EXECUTOR_CONFIG
        )

send_records_to_intercom = PythonOperator(
        dag=dag,
        task_id='send_records_to_intercom',
        python_callable=send_records_to_intercom,
        executor_config=EXECUTOR_CONFIG
        )

export_redshift_data_to_s3 >> send_records_to_intercom
