import datetime
import logging

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'recommerce_masterliste_mapping_step_2'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 3,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('30 7 * * 1-5'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'operations', 'recommerce', 'masterliste_mapping'])

stage_foxway_masterliste_data = AirbyteTriggerSyncOperator(
    task_id='airbyte_stage_foxway_mastliste',
    airbyte_conn_id='airbyte_prod',
    connection_id='2130dab8-bdb7-4cd1-980f-1d406db6b77d',
    asynchronous=False,
    timeout=3600,
    wait_seconds=60,
    dag=dag
)

process_foxway_masterliste_data = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="process_foxway_masterliste_data",
    sql="./sql/recommerce_foxway.sql"
)

stage_foxway_masterliste_data >> process_foxway_masterliste_data
