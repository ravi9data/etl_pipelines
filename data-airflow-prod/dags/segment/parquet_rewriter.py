import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.segment.parquet_rewriter import hour_rewriter
from plugins.config import ENVIRONMENT
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.date_utils import get_hours

DAG_ID = 'segment_parquet_rewriter'

EXECUTOR_CONFIG = {
    'production': {
        'KubernetesExecutor': {
            'request_memory': '10G',
            'request_cpu': '4000m',
            'limit_memory': '10G',
            'limit_cpu': '4000m'
        }
    },
    'staging': {
        'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '300m',
            'limit_memory': '1G',
            'limit_cpu': '1000m'
        }
    }
}

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 11, 6),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=15),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=15)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Re-write Segment raw data as Parquet, renaming some fields.',
    schedule_interval=get_schedule('20,45 * * * *'),
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID, 'segment', 'parquet']
)


hours = get_hours(config_variable_name=DAG_ID, delta_hours=2)


for _i, _h in enumerate(hours):
    amplitude_hour_rewriter = PythonOperator(
        dag=dag,
        task_id=f"hour_rewriter_{_i+1}",
        python_callable=hour_rewriter,
        executor_config=EXECUTOR_CONFIG.get(ENVIRONMENT),
        params={'config_variable_name': DAG_ID,
                'hour': str(_h)}
    )
