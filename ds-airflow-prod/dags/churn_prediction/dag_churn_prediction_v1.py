import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from business_logic.churn_prediction.logic import generate_predictions
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = "churn_prediction_v1"
logger = logging.getLogger(__name__)

EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '3G',
        'request_cpu': '2000m',
        'limit_memory': '5G',
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
    description="Generate Churn Predictions",
    start_date=datetime(2024, 6, 18),
    schedule_interval=get_schedule("0 16 * * 1"),
    max_active_runs=1,
    catchup=False,
    tags=["Automation", "DS"],
)

t0 = DummyOperator(task_id="start", dag=dag)

generate_predictions_task = PythonOperator(
    dag=dag,
    task_id='generate_predictions',
    python_callable=generate_predictions,
    executor_config=EXECUTOR_CONFIG
)

t0 >> generate_predictions_task
