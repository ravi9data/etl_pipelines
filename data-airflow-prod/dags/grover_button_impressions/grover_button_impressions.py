import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback, prepare_dates

DAG_ID = 'grover_button_impressions'
REDSHIFT_CONN = 'redshift'

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 2, 15),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=15),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=10)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Ingest grover button impressions data from spectrum to Redshift',
    schedule_interval=get_schedule('35 * * * *'),
    catchup=False,
    max_active_runs=1,
    tags=[DAG_ID]
)

start_end_dates = PythonOperator(
    dag=dag,
    task_id="start_end_dates",
    python_callable=prepare_dates,
    op_kwargs={
        'variable_start_at': 'grover_button_impressions_start_at',
        'variable_end_at': 'grover_button_impressions_end_at'
    }
 )

staging_table = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="staging_table",
    sql="./sql/data_preparation.sql"
)

upsert = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="upsert",
    sql="./sql/upsert.sql",
    autocommit=False
)

cleanup = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="cleanup",
    sql="./sql/cleanup.sql",
    autocommit=False
)

start_end_dates >> staging_table >> upsert >> cleanup
