import datetime
import logging

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'us_returns_data'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 2,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('0 12-23 * * 1-5'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'operations', 'us', 'returns'])

stage_returns_data = AirbyteTriggerSyncOperator(
    task_id='airbyte_stage_returns_data',
    airbyte_conn_id='airbyte_prod',
    connection_id='75eff998-1489-4df7-ba52-91a32e11e40b',
    asynchronous=False,
    timeout=3600,
    wait_seconds=60,
    dag=dag
)

process_returns_data = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="process_returns_data",
    sql="./sql/process_returns_data.sql"
)

stage_returns_data >> process_returns_data
