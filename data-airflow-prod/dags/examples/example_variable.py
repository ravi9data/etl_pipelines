import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from plugins.dag_utils import on_failure_callback

DAG_ID = 'example_variable'
VAR_NAME = 'example'

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 6, 9),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=5),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=5)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Just an example DAG to show how to use jinja templating',
    max_active_runs=1,
    catchup=False,
    tags=['example', DAG_ID]
)


def my_function(*, my_arg):
    logger.info(type(my_arg))
    logger.info(my_arg)


example_task = PythonOperator(
    dag=dag,
    task_id='example_task',
    python_callable=my_function,
    op_kwargs={
        # to use jinja templating, and picing the VAR_NAME, use this sting concatenation
        'my_arg': ''.join(["{{ var.value.get('", VAR_NAME, "', None) }}"])
    }
)
