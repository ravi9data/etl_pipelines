import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from business_logic.price_elasticity_v1.data_processor import data_processor
from business_logic.price_elasticity_v1.main_data_extractor import \
    load_campaign_data
from business_logic.price_elasticity_v1.price_data import price_data
from business_logic.price_elasticity_v1.sku_date_extractor import \
    processed_campaign_data
from business_logic.price_elasticity_v1.variables import (
    bucket_name, campaign_data_path, campaign_sku_wo_campaign_id_path,
    final_models_path, fuel_prices_url, glue_models_path,
    holidays_de_2022_path, holidays_de_2023_path, price_data_path,
    price_elasticity_schema, price_elasticity_table, price_query_path)
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = "price_elasticity_v1"
GOOGLE_CLOUD_CONN = "google_cloud_default"
logger = logging.getLogger(__name__)

EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '5G',
        'request_cpu': '2000m',
        'limit_memory': '8G',
        'limit_cpu': '4000m'
    }
}

EXECUTOR_CONFIG_2 = {
    'KubernetesExecutor': {
        'request_memory': '15G',
        'request_cpu': '4000m',
        'limit_memory': '15G',
        'limit_cpu': '4000m'
    }
}

default_args = {
    "owner": "ds",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "on_failure_callback": on_failure_callback,
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="Generate Price Elasticity MOdels",
    start_date=datetime(2023, 8, 16),
    schedule_interval=get_schedule("0 16 * * 1"),
    max_active_runs=1,
    catchup=False,
    tags=["Automation", "DS"],
)

t0 = DummyOperator(task_id="start", dag=dag)


main_data_extractor = PythonOperator(
    dag=dag,
    task_id='main_data_extractor',
    python_callable=load_campaign_data,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "data_to_pull": "Campaign Trackers & Reports!A:Z",
               "info": 'main',
               "bucket_name": bucket_name,
               "campaign_data_path": campaign_data_path
               },
    executor_config=EXECUTOR_CONFIG
)

processed_campaign_data = PythonOperator(
    dag=dag,
    task_id='processed_campaign_data',
    python_callable=processed_campaign_data,
    op_kwargs={"bucket_name": bucket_name,
               "campaign_data_path": campaign_data_path,
               "campaign_sku_wo_campaign_id_path": campaign_sku_wo_campaign_id_path
               },
    executor_config=EXECUTOR_CONFIG
)

price_data = PythonOperator(
    dag=dag,
    task_id='price_data',
    python_callable=price_data,
    op_kwargs={"bucket_name": bucket_name,
               "price_query_path": price_query_path,
               "price_data_path": price_data_path
               },
    executor_config=EXECUTOR_CONFIG_2
)

data_processor = PythonOperator(
    dag=dag,
    task_id="data_processor",
    python_callable=data_processor,
    op_kwargs={
        "bucket_name": bucket_name,
        "price_data_path": price_data_path,
        "campaign_sku_wo_campaign_id_path": campaign_sku_wo_campaign_id_path,
        "fuel_prices_url": fuel_prices_url,
        "holidays_de_2022_path": holidays_de_2022_path,
        "holidays_de_2023_path": holidays_de_2023_path,
        "final_models_path": final_models_path,
        "glue_models_path": glue_models_path,
        "price_elasticity_table": price_elasticity_table,
        "price_elasticity_schema": price_elasticity_schema
    },
    executor_config=EXECUTOR_CONFIG_2
)

t0 >> main_data_extractor >> processed_campaign_data >> price_data >> data_processor
