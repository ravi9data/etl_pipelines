import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from business_logic.credit_bureau_reporting.experian_reporting import \
    experian_reporting
from business_logic.credit_bureau_reporting.variables import (
    bucket_name, columns_to_keep, experian_query_path, experian_report_path,
    rename_dict)
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = "experian_reporting_export"
logger = logging.getLogger(__name__)

# Current Date
formatted_date = datetime.now().strftime('%Y%m%d')
FILE_NAME = f"030090{formatted_date}V001.csv"

EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '1G',
        'request_cpu': '500m',
        'limit_memory': '8G',
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
    description="Experian Reporting",
    start_date=datetime(2024, 2, 12),
    schedule_interval=get_schedule("0 8 * * 1"),
    max_active_runs=1,
    catchup=False,
    tags=["Automation", "DS"],
)

t0 = DummyOperator(task_id="start", dag=dag)

experian_reporting = PythonOperator(
    dag=dag,
    task_id='experian_reporting',
    python_callable=experian_reporting,
    op_kwargs={"bucket_name": bucket_name,
               "query_path": experian_query_path,
               "report_path": experian_report_path,
               "csv_filename": FILE_NAME,
               "rename_dict": rename_dict,
               "columns_to_keep": columns_to_keep
               },
    executor_config=EXECUTOR_CONFIG
)

t0 >> experian_reporting
