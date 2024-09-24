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
from plugins.utils.gsheet_s3_to_redshift import load_gsheet_s3_to_redshift

GOOGLE_SHEET_ID = '1Ej_Wjh3jYVufSjpnwzhufy-19c7SRoXxcXns-EDR1_w'
GOOGLE_SHEET_RANGE = f"Load_to_redshift_{datetime.datetime.now().strftime('%Y-%m-%d')}"

REDSHIFT_CONN = 'redshift_default'
S3_BUCKET = 'grover-eu-central-1-production-data-bi-curated'
S3_DESTINATION_KEY = "manual_payment/booking_data.json"


def task_failure_alert(context):
    task_id = context['task_instance'].task_id
    if task_id == "get_manual_payments_data":
        message = 'Manual Payments File was not uploaded today'
        send_notification(message, 'slack')
    if task_id == "load_data_s3_staging":
        message = 'Manual Payments File ERROR : File has no records or the data is not updated!'
        send_notification(message, 'slack')
    else:
        on_failure_callback(context)


default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 2,
                'on_failure_callback': task_failure_alert,
                }

with DAG(
        dag_id="coba_manual_payments_booking",
        schedule_interval=get_schedule('30 9 * * 1-5'),
        default_args=default_args,
        catchup=False,
        tags=['gsheet', 'Manual Payments', 'Coba', 'salesforce'],
) as dag:
    get_manual_payments_data = GoogleApiToS3Operator(
        dag=dag,
        google_api_service_name='sheets',
        google_api_service_version='v4',
        google_api_endpoint_path='sheets.spreadsheets.values.get',
        google_api_endpoint_params={'spreadsheetId': GOOGLE_SHEET_ID, 'range': GOOGLE_SHEET_RANGE},
        s3_destination_key=f's3://{S3_BUCKET}/{S3_DESTINATION_KEY}',
        task_id='get_manual_payments_data',
        gcp_conn_id='google_cloud_default',
        aws_conn_id='aws_default',
        s3_overwrite=True
    )

    load_gsheet_s3_to_redshift = PythonOperator(
        dag=dag,
        task_id='load_data_s3_staging',
        python_callable=load_gsheet_s3_to_redshift,
        provide_context=True,
        do_xcom_push=True,
        op_kwargs={"s3_bucket": S3_BUCKET,
                   "s3_key": S3_DESTINATION_KEY,
                   "target_table": "manual_payments_coba",
                   "target_schema": "stg_external_apis_dl"}
    )

coba_manual_payments = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="00_coba_manual_payments",
    sql="./sql/00_coba_manual_payments.sql"
)
coba_manual_payments_raw = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="01_coba_manual_payments_raw",
    sql="./sql/01_coba_manual_payments_raw.sql"
)

coba_manual_payments_orders = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="02_coba_manual_payments_orders",
    sql="./sql/02_coba_manual_payments_orders.sql"
)

coba_manual_payment_due_match_customer = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="03_coba_manual_payment_due_match_customer",
    sql="./sql/03_coba_manual_payment_due_match_customer.sql"
)

coba_manual_payment_order_id_mapping = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="04_coba_manual_payment_order_id_mapping",
    sql="./sql/04_coba_manual_payment_order_id_mapping.sql"
)

coba_manual_payment_manual_review = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="05_coba_manual_payment_manual_review",
    sql="./sql/05_coba_manual_payment_manual_review.sql"
)

coba_manual_payment_recon = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="06_coba_manual_payment_recon",
    sql="./sql/06_coba_manual_payment_recon.sql"
)

payment_booking_on_sf = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="07_coba_payment_booking_on_sf",
    sql="./sql/07_coba_payment_booking_on_sf.sql"
)

coba_auto_payments_sp_ids = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="08_coba_auto_payments_sp_ids",
    sql="./sql/08_coba_auto_payments_sp_ids.sql"
)

update_payment_booking_data = PythonOperator(
        dag=dag,
        task_id='get_payload',
        python_callable=update_payment_booking_data,
        op_kwargs={'table_name': 'dm_debt_collection.coba_payment_booking_on_sf'}
    )

get_manual_payments_data >> load_gsheet_s3_to_redshift >> coba_manual_payments >> \
    coba_manual_payments_raw >> coba_manual_payments_orders >> \
    coba_manual_payment_due_match_customer >> coba_manual_payment_order_id_mapping >> \
    coba_manual_payment_manual_review >> coba_manual_payment_recon >> \
    payment_booking_on_sf >> coba_auto_payments_sp_ids >> update_payment_booking_data
