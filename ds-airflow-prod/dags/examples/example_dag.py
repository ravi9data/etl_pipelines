import datetime
import logging
import os
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.example_operator import ExampleOperator

DAG_ID = 'example_dag'

logger = logging.getLogger(f'examples.{DAG_ID}')

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 6, 9),
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=5),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=5)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Just an example DAG with custom Operator.',
    schedule_interval=get_schedule('*/15 * * * *'),
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=15),
    tags=['example', 'example_dag', 'example_operator']
)


def print_task(**context):
    _task = context['task']
    _dag = context['dag']
    # _ti = context['ti']
    logger.info(f'Executed from {_task.task_id} inside {_dag.dag_id}')


def print_version():
    version = sys.version
    logger.info(version)

    return version


def print_stage():
    stage = os.environ.get('STAGE')
    logger.info(f'Printing from stage: {stage}')


def print_env():
    env = os.environ
    logger.info(env)


def handle_xcom(**kwargs):
    task_id = 'print_version'
    task_instance = kwargs['ti']
    value = task_instance.xcom_pull(key='return_value', task_ids=task_id)

    logger.info(f'Value returned from {task_id}: {value}')


task_group = TaskGroup(
    dag=dag,
    group_id='just_a_task_group'
)

task_1 = BashOperator(
    dag=dag,
    task_id='print_date',
    bash_command='date'
)

task_2 = PythonOperator(
    dag=dag,
    task_id='print_task',
    python_callable=print_task
)

task_3 = PythonOperator(
    dag=dag,
    task_id='print_version',
    python_callable=print_version,
    task_group=task_group
)

task_4 = PythonOperator(
    dag=dag,
    task_id='xcom_example',
    python_callable=handle_xcom
)

task_5 = ExampleOperator(
    dag=dag,
    task_id='example_operator',
    example_argument='just a test'
)

task_1 >> [task_2, task_3] >> task_4 >> task_5
