import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.curated.kafka import operations_order_allocated
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.date_utils import get_hours

DAG_ID = 'curated_kafka_operations_order_allocated'
CONFIG_VARIABLE_NAME = 'kafka_operations_order_allocated'
GLUE_TABLE_NAME = 'operations_order_allocated'
EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '2G',
        'request_cpu': '1000m',
        'limit_memory': '8G',
        'limit_cpu': '4000m'
    }
}

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 12, 17),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=30)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description=f'Process {DAG_ID} kafka topic to curated layer',
    schedule_interval=get_schedule('*/30 * * * *'),
    catchup=False,
    max_active_runs=1,
    tags=[DAG_ID, 'curated', 'kafka', GLUE_TABLE_NAME]
)

hours = get_hours(CONFIG_VARIABLE_NAME)

for _i, _h in enumerate(hours):
    amplitude_hour_rewriter = PythonOperator(
        dag=dag,
        task_id=f"operations_order_allocated_hour_rewriter_{_i+1}",
        python_callable=operations_order_allocated.operations_order_allocated_hour_rewriter,
        executor_config=EXECUTOR_CONFIG,
        params={'config_variable_name': CONFIG_VARIABLE_NAME,
                'glue_table_name': GLUE_TABLE_NAME,
                'hour': str(_h)}
    )
