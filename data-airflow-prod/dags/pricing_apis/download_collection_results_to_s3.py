import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.pricing_apis.download_collection_results import \
    process_collection_results
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'download_collection_results_to_s3'

EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'limit_memory': '8G',
        'limit_cpu': '2000m'
    }
}

countdown_config = {
    'api_domain': 'api.countdownapi.com',
    'api_key_secret_var': 'countdown_api_key',
    's3_destination': 's3://grover-eu-central-1-production-data-extractions/staging/countdown'
}

rainforest_config = {
    'api_domain': 'api.rainforestapi.com',
    'api_key_secret_var': 'rainforest_api_key',
    's3_destination': 's3://grover-eu-central-1-production-data-extractions/staging/rainforest_api'     # noqa: E501
}

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 1, 30),
    'retries': 0,
    'retry_delay': datetime.timedelta(seconds=15),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=60)
}

dag = DAG(
    dag_id=DAG_ID,
    doc_md=f"""
        ### {DAG_ID}
        - Download the data from api
        - Transforms the JSONL files to Parquet format and registers Glue tables.
        """,
    description='Puts market prices from Ebay in the Lake.',
    tags=['api', 'prices', 'dcm'],
    schedule_interval=get_schedule('30 9 * * 4'),
    catchup=False,
    default_args=default_args,
)

download_countdown_collection = PythonOperator(
    task_id='download_countdown_collection',
    dag=dag,
    python_callable=process_collection_results,
    executor_config=EXECUTOR_CONFIG,
    op_kwargs={
        "run_date": (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d'),
        "api_domain": countdown_config["api_domain"],
        "api_key": f'{{{{ var.value.{countdown_config["api_key_secret_var"]} }}}}',
        "s3_destination": countdown_config['s3_destination']
    }
)

download_rainforest_collection = PythonOperator(
    task_id='download_rainforest_collection',
    dag=dag,
    python_callable=process_collection_results,
    executor_config=EXECUTOR_CONFIG,
    op_kwargs={
        "run_date": (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d'),
        "api_domain": rainforest_config["api_domain"],
        "api_key": f'{{{{ var.value.{rainforest_config["api_key_secret_var"]} }}}}',
        "s3_destination": rainforest_config['s3_destination']
    }
)

download_countdown_collection >> download_rainforest_collection
