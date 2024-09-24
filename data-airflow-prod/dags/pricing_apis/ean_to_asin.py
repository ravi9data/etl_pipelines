import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from business_logic.pricing_apis.asin_converter import asin_converter
from plugins.dag_utils import on_failure_callback

DAG_ID = 'asin_converter'

REDSHIFT_CONN = 'redshift'
config = {
    'api': 'https://api.rainforestapi.com/request',
    'api_key_secret_var': 'rainforest_api_key'
}

EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '13G',
        'request_cpu': '8000m',
        'limit_memory': '15G',
        'limit_cpu': '10000m'
    }
}

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 3, 28),
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=150),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=900)
}

dag = DAG(
    dag_id=DAG_ID,
    doc_md=f"""
        ### {DAG_ID}
        - Get eans for eu region from redshift
        - convert eans for eu region to asin using rainforest_api
        - Get upcs for us region from redshift
        - convert upcs for us region to asin using rainforest_api
        """,
    description='Puts market prices from amazon in the Lake.',
    tags=['ean', 'upcs', 'asin'],
    schedule_interval='30 9 1 * *',
    catchup=False,
    default_args=default_args,
)

clear_task = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id='clear',
    sql='./sql/clear.sql'
)

ean_to_asin_converter = PythonOperator(
    task_id='ean_to_asin',
    dag=dag,
    python_callable=asin_converter,
    op_kwargs={
        "api_config": config,
        "converter": 'ean_to_asin',
        "api_key": f'{{{{ var.value.{config["api_key_secret_var"]} }}}}',
        },
    executor_config=EXECUTOR_CONFIG
)

upcs_to_asin_converter = PythonOperator(
    task_id='upcs_to_asin',
    dag=dag,
    python_callable=asin_converter,
    op_kwargs={
        "api_config": config,
        "converter": 'upcs_to_asin',
        "api_key": f'{{{{ var.value.{config["api_key_secret_var"]} }}}}',
        },
    executor_config=EXECUTOR_CONFIG
)

clear_task >> ean_to_asin_converter >> upcs_to_asin_converter
