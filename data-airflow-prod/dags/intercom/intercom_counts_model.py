import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from intercom.config.counts_model_config import config as counts_model_config

from business_logic.intercom.intercom_counts_model import counts_model_writer
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'intercom_counts_model'
EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '1G',
        'request_cpu': '500m',
        'limit_memory': '4G',
        'limit_cpu': '1000m'
    }
}

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 2, 3),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=120),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=15)
}


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Extract data from Intercom API',
    schedule_interval=get_schedule('0 0 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=['intercom', 'external', 'api', DAG_ID]
)


task_list = []
for conf, i in zip(counts_model_config, range(len(counts_model_config))):
    task_list.append(PythonOperator(
        dag=dag,
        task_id=f'load_{conf.get("task_name")}',
        python_callable=counts_model_writer,
        op_kwargs=conf,
        executor_config=EXECUTOR_CONFIG
        ))

    if i not in [0]:
        task_list[i-1] >> task_list[i]
