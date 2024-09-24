import datetime
import logging

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'spl_kd_cleanup_channeling_mapping'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 3,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('30 16 * * 1'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'spl', 'kd', 'operations', 'channeling_mapping'])

stage_spl_kd_channel_mapping_data = AirbyteTriggerSyncOperator(
    task_id='airbyte_stage_spl_kd_channel_mapping',
    airbyte_conn_id='airbyte_prod',
    connection_id='75d04251-911f-4392-a517-fffeb881db85',
    asynchronous=False,
    timeout=3600,
    wait_seconds=60,
    dag=dag
)

process_spl_kd_channel_mapping_data = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="process_spl_kd_channel_mapping_data",
    sql="./sql/spl_kd_final.sql"
)

stage_spl_kd_channel_mapping_data >> process_spl_kd_channel_mapping_data
