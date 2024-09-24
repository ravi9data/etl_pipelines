import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.widgets import widget_data
from dags.widgets.config import widget_config
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'widgets'
REDSHIFT_CONN = 'redshift'

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 11, 15),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=10),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=10)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Ingest widget data in S3',
    schedule_interval=get_schedule('47 */2 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID]
)

for kw_args in widget_config:
    widget_data_tasks = PythonOperator(
        dag=dag,
        task_id=f'widget_data_{kw_args["prefix"][:-1]}',
        python_callable=widget_data,
        op_kwargs=kw_args
    )
