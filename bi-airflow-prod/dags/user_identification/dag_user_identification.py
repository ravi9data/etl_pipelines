import datetime
import logging

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'es_user_identification'

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 11, 9),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=30)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='table for user identification for payment ops',
    schedule_interval=get_schedule("0 7 * * 0-6"),
    max_active_runs=1,
    catchup=False,
    tags=['ES', DAG_ID, 'user', 'identification']
)

user_identification = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="user_identification",
    sql="./sql/user_identification.sql"
)
