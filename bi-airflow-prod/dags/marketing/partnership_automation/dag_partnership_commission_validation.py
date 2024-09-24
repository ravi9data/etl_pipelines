import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.suite.transfers.sql_to_sheets import \
    SQLToGoogleSheetsOperator

from dags.marketing.partnership_automation.sql.everflow_orders_sql import (
    ef_everflow_orders, ef_everflow_orders_historical)
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.utils.gsheet import clear_gsheet

DAG_ID = 'partnership_commision_validation'
REDSHIFT_CONN = 'redshift_default'
GOOGLE_CLOUD_CONN = 'google_cloud_default'
EF_SPREADSHEET_ID = '1qj30o-fdb3vR0enmN8_3bM8GvRg2uuOPWJ43nOyNwBg'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng', 'depends_on_past': False,
                'start_date': datetime.datetime(2022, 8, 3),
                'retries': 2,
                'retry_delay': datetime.timedelta(seconds=15),
                'retry_exponential_backoff': True,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          description='A support DAG to unload dataset for ml to S3',
          schedule_interval=get_schedule('30 8 15 * 1'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'partnership'])

clean_everflow_sheet = PythonOperator(
    dag=dag,
    task_id="clean_everflow_sheet",
    python_callable=clear_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN, "sheet_id": EF_SPREADSHEET_ID,
               "range_name": ["everflow_orders", "everflow_orders_historical"]}
)

send_everflow_orders = SQLToGoogleSheetsOperator(
    dag=dag,
    task_id="send_everflow_orders",
    sql=ef_everflow_orders,
    spreadsheet_id=EF_SPREADSHEET_ID,
    spreadsheet_range="everflow_orders",
    sql_conn_id=REDSHIFT_CONN,
    gcp_conn_id=GOOGLE_CLOUD_CONN
)

send_everflow_orders_historical = SQLToGoogleSheetsOperator(
    dag=dag,
    task_id="send_everflow_orders_historical",
    sql=ef_everflow_orders_historical,
    spreadsheet_id=EF_SPREADSHEET_ID,
    spreadsheet_range="everflow_orders_historical",
    sql_conn_id=REDSHIFT_CONN,
    gcp_conn_id=GOOGLE_CLOUD_CONN
)

clean_everflow_sheet >> send_everflow_orders >> send_everflow_orders_historical
