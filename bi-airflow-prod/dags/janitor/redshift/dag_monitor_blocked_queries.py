import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from dags.janitor.redshift.get_blocked_queries import get_blocked_queries
from plugins.dag_utils import on_failure_callback

DAG_ID = 'monitor_blocked_queries'
REDSHIFT_CONN = 'redshift_admin'

ENABLE_DELETE = True
PRINT_DELETES = True

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 10, 23),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=5)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    tags=[DAG_ID, 'maintenance_dag']
)

monitor_blocked_queries = PythonOperator(
    dag=dag,
    task_id='monitor_blocked_queries',
    python_callable=get_blocked_queries,
)
