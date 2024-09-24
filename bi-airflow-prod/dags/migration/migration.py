import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import on_failure_callback

REDSHIFT_CONN = 'redshift_default'

default_args = {'owner': 'bi-eng', 'depends_on_past': False,
                'start_date': datetime.datetime(2022, 8, 8), 'retries': 2,
                'retry_delay': datetime.timedelta(seconds=15), 'retry_exponential_backoff': True,
                'on_failure_callback': on_failure_callback}

dag = DAG(
    dag_id="migration_job",
    schedule_interval='0 23 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['migration', 'B2B'],
)


task_process_ticket_data_sql = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="upsert_migration_data",
    sql="./sql/migration_tables.sql"
)
