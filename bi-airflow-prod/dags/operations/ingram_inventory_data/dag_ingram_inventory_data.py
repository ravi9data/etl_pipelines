import datetime
import logging

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'ingram_inventory_data'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 2,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('0 3 * * 1-5'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'operations', 'Ingram', 'Inventory'])

stage_inventory_data = AirbyteTriggerSyncOperator(
    task_id='airbyte_stage_inventory_data',
    airbyte_conn_id='airbyte_prod',
    connection_id='de7c04af-f956-4dcf-8b1b-d3a9a9dca05c',
    asynchronous=False,
    timeout=7200,
    wait_seconds=60,
    dag=dag
)

process_inventory_data = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="process_inventory_data",
    sql="./sql/process_inventory_data.sql"
)

stage_inventory_data >> process_inventory_data
