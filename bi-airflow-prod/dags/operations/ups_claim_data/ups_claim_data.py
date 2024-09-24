import datetime
import logging

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'ups_claim_data'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 11, 5),
                'retries': 3,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('0 0 5 * *'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'operations', 'ups', 'claim_data'])

stage_ups_claim_data = AirbyteTriggerSyncOperator(
    task_id='airbyte_stage_ups_claim_data',
    airbyte_conn_id='airbyte_prod',
    connection_id='df0fbacc-61dc-4f0f-b311-d3241bfe02a1',
    asynchronous=False,
    timeout=3600,
    wait_seconds=60,
    dag=dag
)

ups_claim_data = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="ups_claim_data",
    sql="./sql/ups_claim_data.sql"
)

stage_ups_claim_data >> ups_claim_data
