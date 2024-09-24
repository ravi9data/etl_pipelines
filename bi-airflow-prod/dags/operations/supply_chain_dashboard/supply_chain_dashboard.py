import datetime
import logging

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'supply_chain_dashboard'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 11, 5),
                'retries': 3,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('50 6,7,8,9,12,15 * * *'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'operations', 'supply', 'chain', 'dashboard'])

stage_supply_chain_dashboard = AirbyteTriggerSyncOperator(
    task_id='airbyte_stage_supply_chain_dashboard',
    airbyte_conn_id='airbyte_prod',
    connection_id='1c08be46-7dba-4150-9d65-864aa2fd0049',
    asynchronous=False,
    timeout=3600,
    wait_seconds=60,
    dag=dag
)

ups_klarfalle = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="ups_klarfalle",
    sql="./sql/ups_klarfalle.sql"
)

eu_Inbound_schedule = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="eu_Inbound_schedule",
    sql="./sql/eu_Inbound_schedule.sql"
)


rejected_deliveries = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="rejected_deliveries",
    sql="./sql/rejected_deliveries.sql"
)


stage_supply_chain_dashboard >> ups_klarfalle >> eu_Inbound_schedule \
    >> rejected_deliveries
