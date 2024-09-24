import datetime
import logging

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.google.suite.transfers.sql_to_sheets import \
    SQLToGoogleSheetsOperator

from dags.marketing.affiliate_commision_validations.sql.cj_sql import (
    cj_eu_status, cj_us_status)
from dags.marketing.affiliate_commision_validations.sql.tradedoubler_sql import \
    td_pending_sql
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.utils.gsheet import clear_gsheet

DAG_ID = 'affiliate_pending_validation'
REDSHIFT_CONN = 'redshift_default'
GOOGLE_CLOUD_CONN = 'google_cloud_default'
CJ_SPREADSHEET_ID = '1zQ6VtiWMryiHX-w50866y04ohVdLfzRT939o1Kiz6G4'
TD_SPREADSHEET_ID = '10EA3t-MF7fsyE2O65uz47gE9OlkYveaii7xWyGZ5xco'

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
          schedule_interval=get_schedule('15 8 * * 1'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'affiliate', 'affiliate_automation', 'affiliate_order_validation'])

clean_cj_sheet = PythonOperator(
    dag=dag,
    task_id="clean_cj_sheet",
    python_callable=clear_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN, "sheet_id": CJ_SPREADSHEET_ID,
               "range_name": ["cj_eu_status", "cj_us_status"]}
)

send_cj_eu_orders = SQLToGoogleSheetsOperator(
    dag=dag,
    task_id="send_cj_eu_orders",
    sql=cj_eu_status,
    spreadsheet_id=CJ_SPREADSHEET_ID,
    spreadsheet_range="cj_eu_status",
    sql_conn_id=REDSHIFT_CONN,
    gcp_conn_id=GOOGLE_CLOUD_CONN
)

send_cj_us_orders = SQLToGoogleSheetsOperator(
    dag=dag,
    task_id="send_cj_us_orders",
    sql=cj_us_status,
    spreadsheet_id=CJ_SPREADSHEET_ID,
    spreadsheet_range="cj_us_status",
    sql_conn_id=REDSHIFT_CONN,
    gcp_conn_id=GOOGLE_CLOUD_CONN
)

clean_tradedoubler_sheet = PythonOperator(
    dag=dag,
    task_id="clean_tradedoubler_sheet",
    python_callable=clear_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN, "sheet_id": TD_SPREADSHEET_ID,
               "range_name": ["pending_orders", "Sheet1"]}
)

send_tradedoubler_orders = SQLToGoogleSheetsOperator(
    dag=dag,
    task_id="send_tradedoubler_orders",
    sql=td_pending_sql,
    spreadsheet_id=TD_SPREADSHEET_ID,
    spreadsheet_range="pending_orders",
    sql_conn_id=REDSHIFT_CONN,
    gcp_conn_id=GOOGLE_CLOUD_CONN
)

chain(
    clean_cj_sheet,
    send_cj_eu_orders,
    send_cj_us_orders,
    clean_tradedoubler_sheet,
    send_tradedoubler_orders
)
