import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.google_api_to_s3 import \
    GoogleApiToS3Operator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from business_logic.manual_payments_booking.update_sf import \
    update_payment_booking_data
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.slack_utils import send_notification
from plugins.utils.gsheet import add_range_and_export_to_gsheet
from plugins.utils.gsheet_s3_to_redshift import load_gsheet_s3_to_redshift

GOOGLE_SHEET_ID = '1j_ZrmLpAYIUVQ-2aM3rzRsaT_SOis2kARCSy1gkpG-Y'
GOOGLE_SHEET_RANGE = f"{datetime.datetime.now().strftime('%Y-%m-%d')}"

GOOGLE_CLOUD_CONN = "google_cloud_default"
BNP_MANUAL_REVIEWS_V1 = '1zaCaf5gK_C0TQqhwx9kaJkd7xJ8smpAUhEUp0Scu-Rs'

REDSHIFT_CONN = 'redshift_default'
S3_BUCKET = 'grover-eu-central-1-production-data-bi-curated'
S3_DESTINATION_KEY = "manual_payments_bnp/booking_data_bnp.json"


def task_failure_alert(context):
    task_id = context['task_instance'].task_id
    if task_id == "get_manual_payments_bnp_data_to_s3":
        message = 'Manual Payments BNP File was not uploaded today'
        send_notification(message, 'slack')
    if task_id == "load_data_from_s3_into_payment_schema_redshift":
        message = 'Manual Payments BNP File ERROR : File has no records or the data is not updated!'
        send_notification(message, 'slack')
    else:
        on_failure_callback(context)


default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2024, 8, 6, 10, 0),
                'retries': 2,
                'retry_delay': datetime.timedelta(seconds=45),
                'on_failure_callback': task_failure_alert,
                'execution_timeout': datetime.timedelta(minutes=20)
                }

with DAG(
        dag_id="bnp_manual_payments_booking",
        schedule_interval=get_schedule('0 10 * * 1-5'),
        default_args=default_args,
        catchup=False,
        tags=['gsheet', 'payments', 'bnp_payments', 'bnp',
              'BNP_paribas', 'salesforce', 'manual_payments'],
) as dag:
    get_manual_payments_bnp_data_to_s3 = GoogleApiToS3Operator(
        dag=dag,
        google_api_service_name='sheets',
        google_api_service_version='v4',
        google_api_endpoint_path='sheets.spreadsheets.values.get',
        google_api_endpoint_params={'spreadsheetId': GOOGLE_SHEET_ID,
                                    'range': GOOGLE_SHEET_RANGE},
        s3_destination_key=f's3://{S3_BUCKET}/{S3_DESTINATION_KEY}',
        task_id='get_manual_payments_bnp_data_to_s3',
        gcp_conn_id='google_cloud_default',
        aws_conn_id='aws_default',
        s3_overwrite=True
    )

    load_gsheet_from_s3_to_redshift = PythonOperator(
        dag=dag,
        task_id='load_data_from_s3_into_payment_schema_redshift',
        python_callable=load_gsheet_s3_to_redshift,
        provide_context=True,
        do_xcom_push=True,
        op_kwargs={"s3_bucket": S3_BUCKET,
                   "s3_key": S3_DESTINATION_KEY,
                   "target_table": "bnp_manual_payments_input_list",
                   "target_schema": "payment"}
    )

bnp_manual_payments_historical = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="00_bnp_manual_payments_input_list_historical",
    sql="./sql/00_bnp_manual_payments_input_list_historical.sql"
)

bnp_manual_payments_raw = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="01_bnp_manual_payments_raw",
    sql="./sql/01_bnp_manual_payments_raw.sql"
)

bnp_manual_payments_orders = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="02_bnp_manual_payments_orders",
    sql="./sql/02_bnp_manual_payments_orders.sql"
)

bnp_manual_payment_due_match_customer = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="03_bnp_manual_payments_due_match_customer",
    sql="./sql/03_bnp_manual_payments_due_match_customer.sql"
)

bnp_manual_payment_order_id_mapping = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="04_bnp_manual_payments_order_id_mapping",
    sql="./sql/04_bnp_manual_payments_order_id_mapping.sql"
)

bnp_manual_payment_manual_review = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="05_bnp_manual_payments_manual_review",
    sql="./sql/05_bnp_manual_payments_manual_review.sql"
)

bnp_manual_payment_recon = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="06_bnp_manual_payments_recon",
    sql="./sql/06_bnp_manual_payments_recon.sql"
)

payment_booking_on_sf = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="07_bnp_manual_payments_booking_on_sf",
    sql="./sql/07_bnp_manual_payments_booking_on_sf.sql"
)

bnp_pushing_non_allocable_transactions = PythonOperator(
    dag=dag,
    task_id="bnp_pushing_non_allocable_transactions",
    python_callable=add_range_and_export_to_gsheet,
    op_kwargs={
        "conn_id": GOOGLE_CLOUD_CONN,
        "sheet_id": BNP_MANUAL_REVIEWS_V1,
        "range_name": GOOGLE_SHEET_RANGE,
        "sql": "select * from payment.bnp_manual_payments_manual_review",
        "include_header": True,
    }
)

bnp_auto_payments_sp_ids = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="08_bnp_manual_payments_auto_payments_sp_ids",
    sql="./sql/08_bnp_manual_payments_auto_payments_sp_ids.sql"
)

update_payment_sf_booking_data = PythonOperator(
        dag=dag,
        task_id='update_salesforce',
        python_callable=update_payment_booking_data,
        op_kwargs={'table_name': 'payment.bnp_manual_payments_booking_on_sf'}
    )

# pylint: disable=W0104
get_manual_payments_bnp_data_to_s3 >> load_gsheet_from_s3_to_redshift >> \
    bnp_manual_payments_historical >> bnp_manual_payments_raw >> \
    bnp_manual_payments_orders >> bnp_manual_payment_due_match_customer >> \
    bnp_manual_payment_order_id_mapping >> bnp_manual_payment_manual_review >> \
    bnp_manual_payment_recon >> payment_booking_on_sf >> bnp_pushing_non_allocable_transactions >> \
    bnp_auto_payments_sp_ids >> update_payment_sf_booking_data
