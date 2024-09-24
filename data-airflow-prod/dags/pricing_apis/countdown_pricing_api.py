import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.pricing_apis.collections import \
    create_countdown_collections
from plugins.dag_utils import on_failure_callback

DAG_ID = 'countdown_pricing_api'

config = {
    'api_domain': 'api.countdownapi.com',
    'api_key_secret_var': 'countdown_api_key',
    'input_list_sql_template': './business_logic/pricing_apis/sql/countdown_api_input_list.sql',
    'destination_id': 'C518235F',
    'input_list_sql_params': None
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
        - Gets latest product data from Redshift
        - Creates parameters for Collections pricing APIs from product data.
        - Starts the Collections.
        - Waits for the Collections to finish running.
        - Transforms the JSONL files to Parquet format and registers Glue tables.
        """,
    description='Puts market prices from Ebay in the Lake.',
    tags=['api', 'prices', 'dcm'],
    schedule_interval='30 9 * * 3',
    catchup=False,
    default_args=default_args,
)

trigger_collection = PythonOperator(
    task_id='trigger_collection',
    dag=dag,
    python_callable=create_countdown_collections,
    op_kwargs={
        "api_config": config,
        "api_key": f'{{{{ var.value.{config["api_key_secret_var"]} }}}}'
    }
)
