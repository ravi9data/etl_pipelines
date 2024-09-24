import datetime
import logging

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_ftp import S3ToFTPOperator
from airflow.providers.google.suite.transfers.sql_to_sheets import \
    SQLToGoogleSheetsOperator

from dags.marketing.affiliate_commision_validations.sql.cj_sql import (
    cj_eu_status_final, cj_us_status_final)
from dags.marketing.affiliate_commision_validations.sql.daisycon_sql import (
    dc_cancelled_orders, dc_new_paid_orders, dc_new_recurring_orders)
from dags.marketing.affiliate_commision_validations.sql.everflow_sql import (
    ef_payload_revenue, ef_status)
from dags.marketing.affiliate_commision_validations.sql.tradedoubler_sql import \
    td_unload_sql
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.redshift.redshift_unload_operator import \
    RedshiftUnloadOperator
from plugins.utils.gsheet import clear_gsheet

DAG_ID = 'affiliate_commision_validation'
REDSHIFT_CONN = 'redshift_default'
GOOGLE_CLOUD_CONN = 'google_cloud_default'
SPREADSHEET_ID = '1nzoKhSFfWWtvLYqlAQchFzgEgaAc2Hx-A293_HU1PUE'
EF_SPREADSHEET_ID = '1lenGN989lzqjfbYDtKKgPFkzvzCcinQ3QLdVG_v6WnI'
CJ_SPREADSHEET_ID = '1zQ6VtiWMryiHX-w50866y04ohVdLfzRT939o1Kiz6G4'

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
          schedule_interval=get_schedule('30 8 15 * *'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'affiliate', 'affiliate_automation', 'affiliate_order_validation'])

unload_tradedoubler_data = RedshiftUnloadOperator(
    dag=dag,
    task_id="unload_tradedoubler_data",
    execution_timeout=datetime.timedelta(minutes=3),
    redshift_conn_id=REDSHIFT_CONN,
    select_query=str(td_unload_sql).replace("'", "''"),
    iam_role='{{var.value.redshift_iam_role}}',
    s3_bucket='{{var.value.s3_bucket_curated_bi}}',
    s3_prefix='affiliate/tradedoubler/push/unload.csv',
    unload_options=['PARALLEL FALSE', 'ALLOWOVERWRITE', 'CSV', 'HEADER'])

send_tradedoubler_data = S3ToFTPOperator(
    dag=dag,
    task_id="send_tradedoubler_data",
    ftp_path="/Grover/validated_orders.csv",
    ftp_conn_id='ftp_tradedoubler',
    s3_bucket='{{var.value.s3_bucket_curated_bi}}',
    s3_key='affiliate/tradedoubler/push/unload.csv000'
)

clean_daisycon_sheet = PythonOperator(
    dag=dag,
    task_id="clean_daisycon_sheet",
    python_callable=clear_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN, "sheet_id": SPREADSHEET_ID,
               "range_name": ["recurring_paid_orders", "new_paid_orders", "cancelled_orders"]}
)

send_daisycon_data_new_recurring_orders = SQLToGoogleSheetsOperator(
    dag=dag,
    task_id="send_daisycon_data_new_recurring_orders",
    sql=dc_new_recurring_orders,
    spreadsheet_id=SPREADSHEET_ID,
    spreadsheet_range="recurring_paid_orders",
    sql_conn_id=REDSHIFT_CONN,
    gcp_conn_id=GOOGLE_CLOUD_CONN
)

send_daisycon_data_new_paid_orders = SQLToGoogleSheetsOperator(
    dag=dag,
    task_id="send_daisycon_data_new_paid_orders",
    sql=dc_new_paid_orders,
    spreadsheet_id=SPREADSHEET_ID,
    spreadsheet_range="new_paid_orders",
    sql_conn_id=REDSHIFT_CONN,
    gcp_conn_id=GOOGLE_CLOUD_CONN
)

send_daisycon_data_cancelled_orders = SQLToGoogleSheetsOperator(
    dag=dag,
    task_id="send_daisycon_data_cancelled_orders",
    sql=dc_cancelled_orders,
    spreadsheet_id=SPREADSHEET_ID,
    spreadsheet_range="cancelled_orders",
    sql_conn_id=REDSHIFT_CONN,
    gcp_conn_id=GOOGLE_CLOUD_CONN
)

clean_everflow_sheet = PythonOperator(
    dag=dag,
    task_id="clean_everflow_sheet",
    python_callable=clear_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN, "sheet_id": EF_SPREADSHEET_ID,
               "range_name": ["payload_revenue", "status"]}
)

send_everflow_payload_revenue = SQLToGoogleSheetsOperator(
    dag=dag,
    task_id="send_everflow_payload_revenue",
    sql=ef_payload_revenue,
    spreadsheet_id=EF_SPREADSHEET_ID,
    spreadsheet_range="payload_revenue",
    sql_conn_id=REDSHIFT_CONN,
    gcp_conn_id=GOOGLE_CLOUD_CONN
)

send_everflow_status = SQLToGoogleSheetsOperator(
    dag=dag,
    task_id="send_everflow_status",
    sql=ef_status,
    spreadsheet_id=EF_SPREADSHEET_ID,
    spreadsheet_range="status",
    sql_conn_id=REDSHIFT_CONN,
    gcp_conn_id=GOOGLE_CLOUD_CONN
)

clean_cj_sheet = PythonOperator(
    dag=dag,
    task_id="clean_cj_sheet",
    python_callable=clear_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN, "sheet_id": CJ_SPREADSHEET_ID,
               "range_name": ["cj_eu_status_final", "cj_us_status_final"]}
)

send_cj_eu_orders = SQLToGoogleSheetsOperator(
    dag=dag,
    task_id="send_cj_eu_orders",
    sql=cj_eu_status_final,
    spreadsheet_id=CJ_SPREADSHEET_ID,
    spreadsheet_range="cj_eu_status_final",
    sql_conn_id=REDSHIFT_CONN,
    gcp_conn_id=GOOGLE_CLOUD_CONN
)

send_cj_us_orders = SQLToGoogleSheetsOperator(
    dag=dag,
    task_id="send_cj_us_orders",
    sql=cj_us_status_final,
    spreadsheet_id=CJ_SPREADSHEET_ID,
    spreadsheet_range="cj_us_status_final",
    sql_conn_id=REDSHIFT_CONN,
    gcp_conn_id=GOOGLE_CLOUD_CONN
)

chain(
    unload_tradedoubler_data,
    send_tradedoubler_data,
    clean_daisycon_sheet,
    send_daisycon_data_new_recurring_orders,
    send_daisycon_data_new_paid_orders,
    send_daisycon_data_cancelled_orders,
    clean_everflow_sheet,
    send_everflow_payload_revenue,
    send_everflow_status,
    clean_cj_sheet,
    send_cj_eu_orders,
    send_cj_us_orders
)
