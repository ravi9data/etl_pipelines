import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'data_science_test'
REDSHIFT_CONN = 'redshift'

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 11, 15),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=5),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=10)
}


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='get latest ',
    schedule_interval=get_schedule('0 8,13,17 * * 1-6'),
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID]
)


test = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="test",
    sql="./sql/test.sql"
)

test
