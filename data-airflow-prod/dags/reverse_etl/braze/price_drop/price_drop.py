import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.reverse_etl.braze.price_drop.data_manipulation import (
    aggregate_joined_data_write_to_s3, calculate_deltas_write_to_s3,
    joined_user_product_data_write_to_s3, send_data_deltas_wrapper,
    send_data_wrapper)
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'braze_price_drop'
REDSHIFT_CONN = 'redshift'
EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '300Mi',
        'request_cpu': '100m',
        'limit_memory': '2G',
        'limit_cpu': '2000m'
    }
}

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 1, 20),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=15),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=120)
}

dag_tags = ['braze', 'price drop', 'reverse_etl', DAG_ID]

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Executes the price drop Strategy sending data to Braze',
    # avoid to run at HH:30, as source tables are fetched at that time
    schedule_interval=get_schedule('30 * * * *'),
    catchup=False,
    max_active_runs=1,
    tags=dag_tags
)

joined_user_product_data_task = PythonOperator(
    dag=dag,
    task_id='joined_user_product_data',
    python_callable=joined_user_product_data_write_to_s3,
    executor_config=EXECUTOR_CONFIG
)

agg_joined_data_task = PythonOperator(
    dag=dag,
    task_id='agg_joined_data',
    python_callable=aggregate_joined_data_write_to_s3,
    executor_config=EXECUTOR_CONFIG
)

send_data_to_braze_task = PythonOperator(
    dag=dag,
    task_id='send_data_to_braze',
    python_callable=send_data_wrapper,
    executor_config=EXECUTOR_CONFIG
)

calculate_deltas_task = PythonOperator(
    dag=dag,
    task_id='calculate_deltas',
    python_callable=calculate_deltas_write_to_s3,
    executor_config={
        'KubernetesExecutor': {
            'request_memory': '7G',
            'request_cpu': '2000m',
            'limit_memory': '10G',
            'limit_cpu': '3000m'
        }
    }
)

send_deltas_task = PythonOperator(
    dag=dag,
    task_id='send_deltas_to_braze',
    python_callable=send_data_deltas_wrapper,
    executor_config={
        'KubernetesExecutor': {
            'request_memory': '1500M',
            'request_cpu': '200m',
            'limit_memory': '3G',
            'limit_cpu': '2000m'
        }
    }
)


joined_user_product_data_task >> agg_joined_data_task >> send_data_to_braze_task >> \
    calculate_deltas_task >> send_deltas_task
