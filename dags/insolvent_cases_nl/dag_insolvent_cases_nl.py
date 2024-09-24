import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.utils.gsheet import add_range_and_export_to_gsheet

# Constants
DAG_ID = 'insolvent_cases_nl'
REDSHIFT_CONN = 'redshift_default'
GOOGLE_CLOUD_CONN = "google_cloud_default"
DESTINATION_SHEET_ID = "1XWEgVHDf1wQHhIS629kO7SAoA3Z24onpAbUWOO2V3Ms"
DESTINATION_SHEET_RANGE_DEFAULT = "Sheet1"

# Logger
logger = logging.getLogger(__name__)

# Default Arguments for the DAG
default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 20),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback
}

# DAG Definition
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='DAG to load and process insolvent cases data',
    schedule_interval=get_schedule('0 11 * * 1-5'),
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID, 'Insolvent', 'DC']
) as dag:

    # Task: Load insolvent data into Redshift via Airbyte
    load_insolvent_data_redshift = AirbyteTriggerSyncOperator(
        task_id="load_insolvent_data_redshift",
        airbyte_conn_id='airbyte_prod',
        connection_id='051bde50-6ab2-4302-bc71-ed5c8db789b6'
    )

    # Task: Run SQL script to process the data
    process_insolvent_cases_sql = PostgresOperator(
        task_id="process_insolvent_cases_sql",
        postgres_conn_id=REDSHIFT_CONN,
        sql="./sql/insolvent_cases_nl.sql"
    )

    # Task: Export the processed data to Google Sheets
    export_to_gsheet = PythonOperator(
        task_id="export_insolvent_cases_to_gsheet",
        python_callable=add_range_and_export_to_gsheet,
        op_kwargs={
            "conn_id": GOOGLE_CLOUD_CONN,
            "sheet_id": DESTINATION_SHEET_ID,
            "range_name": DESTINATION_SHEET_RANGE_DEFAULT,
            "sql": "SELECT * FROM dm_debt_collection.insolvent_cases_nl",
            "include_header": True,
        }
    )

    # Task Dependencies
    load_insolvent_data_redshift >> process_insolvent_cases_sql >> export_to_gsheet
