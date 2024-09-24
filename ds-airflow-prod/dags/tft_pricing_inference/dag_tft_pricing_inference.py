import logging
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from business_logic.tft_pricing_inference.inference_model import \
    run_tft_price_elasticity
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = "tft_price_inference"

logger = logging.getLogger(__name__)

EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '12G',
        'request_cpu': '4000m',
        'limit_memory': '12G',
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
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2023, 8, 16),
    schedule_interval=get_schedule("0 18 * * 1"),
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID, "pricing inference"],
)

t0 = DummyOperator(task_id="start", dag=dag)

run_tft_price_elasticity = PythonOperator(
    dag=dag,
    task_id="price_elasticity",
    python_callable=run_tft_price_elasticity,
    executor_config=EXECUTOR_CONFIG
)

t0 >> run_tft_price_elasticity
