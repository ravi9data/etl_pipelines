import datetime
import logging

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'ingram_kpis_mapping'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 3,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('0 10 * * *'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'operations', 'Ingram', 'kpi', 'mapping'])

stage_ingram_kpis_mapping_data = AirbyteTriggerSyncOperator(
    task_id='airbyte_stage_ingram_kpis_mapping_data',
    airbyte_conn_id='airbyte_prod',
    connection_id='34f1d2d4-cfed-416f-a45a-1e7c86bf31ef',
    asynchronous=False,
    timeout=3600,
    wait_seconds=60,
    dag=dag
)

process_ingram_kpis_mapping_data = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="process_ingram_kpis_mapping_data",
    sql="./sql/ingram_kpis_mapping.sql"
)

stage_ingram_kpis_mapping_data >> process_ingram_kpis_mapping_data
