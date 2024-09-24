import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from dags.janitor.redshift.redshift_maintenance_ops import run_vacuum
from plugins.dag_utils import on_failure_callback

DAG_ID = 'db_vacuum_analyze'

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 11, 20),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=360)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval='0 10 * * SUN',
    tags=[DAG_ID, 'maintenance_dag']
)

run_vacuum_analyze = PythonOperator(
    dag=dag,
    task_id='run_vacuum_analyze',
    python_callable=run_vacuum,
)
