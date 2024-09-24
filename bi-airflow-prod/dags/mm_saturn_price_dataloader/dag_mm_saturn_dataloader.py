import datetime
import logging
from datetime import timedelta

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'mm_saturn_price_dataloader'
REDSHIFT_CONN = 'redshift_default'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 9, 20),
                'retries': 2,
                'retry_delay': timedelta(seconds=15),
                'retry_exponential_backoff': True,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID, default_args=default_args,
          description='DAG to load Media Market and Satrun pricing data from HTTPS',
          schedule_interval=get_schedule('0 10 * * *'), max_active_runs=1, catchup=False,
          tags=[DAG_ID, 'pricing', 'commercial'])

download_mm_de_pricing_data = AirbyteTriggerSyncOperator(
    dag=dag,
    task_id="download_mm_de_pricing_data",
    airbyte_conn_id='airbyte_prod',
    connection_id='2f23c8df-809f-4ac8-a9ea-35751efcd3da',
    asynchronous=False,
    timeout=1200,
    wait_seconds=300
)

download_mm_es_pricing_data = AirbyteTriggerSyncOperator(
    dag=dag,
    task_id="download_mm_es_pricing_data",
    airbyte_conn_id='airbyte_prod',
    connection_id='34faefa9-f972-490f-8f35-3699a0a22a38',
    asynchronous=False,
    timeout=1200,
    wait_seconds=300
)

download_mm_at_pricing_data = AirbyteTriggerSyncOperator(
    dag=dag,
    task_id="download_mm_at_pricing_data",
    airbyte_conn_id='airbyte_prod',
    connection_id='e7d149ee-6878-47b3-9873-29c945f44aea',
    asynchronous=False,
    timeout=1200,
    wait_seconds=300
)

download_sat_de_pricing_data = AirbyteTriggerSyncOperator(
    dag=dag,
    task_id="download_sat_de_pricing_data",
    airbyte_conn_id='airbyte_prod',
    connection_id='88bd9afa-8e07-4f39-bce0-2503cbb13607',
    asynchronous=False,
    timeout=12000,
    wait_seconds=300
)

run_mediamarkt_price_data_sql = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="process_mediamarket_data",
    sql="./sql/mediamarkt_price_data.sql"
)

run_saturn_price_data_sql = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="process_saturn_data",
    sql="./sql/saturn_price_data.sql"
)

run_mediamarkt_es_price_data_sql = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="process_mediamarket_data_es",
    sql="./sql/mediamarkt_price_data_es.sql"
)

run_mediamarkt_at_price_data_sql = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="process_mediamarket_data_at",
    sql="./sql/mediamarkt_price_data_at.sql"
)

download_mm_de_pricing_data >> run_mediamarkt_price_data_sql
download_mm_es_pricing_data >> run_mediamarkt_es_price_data_sql
download_mm_at_pricing_data >> run_mediamarkt_at_price_data_sql
download_sat_de_pricing_data >> run_saturn_price_data_sql
