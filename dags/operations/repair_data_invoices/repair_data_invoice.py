import datetime
import logging

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'repair_data_invoices'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 2,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('0 9 1 * * '),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'operations', 'repair', 'Invoices'])

stage_repair_data = AirbyteTriggerSyncOperator(
    task_id='airbyte_stage_repair_data',
    airbyte_conn_id='airbyte_prod',
    connection_id='d45ef3c7-0a36-4b3c-b70c-3c47bacb3413',
    asynchronous=False,
    timeout=3600,
    wait_seconds=60,
    dag=dag
)

process_repair_data = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="process_repair_invoices",
    sql="./sql/repair_invoices.sql"
)

stage_repair_data >> process_repair_data
