import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.google_api_to_s3 import \
    GoogleApiToS3Operator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import on_failure_callback
from plugins.utils.gsheet_s3_to_redshift import \
    load_gsheet_s3_to_redshift_in_append

GOOGLE_SHEET_ID = '17ZNz0wyai2U5BAbIStzJoxtJw-MATNvCAHayCA3cAq4'
GOOGLE_SHEET_RANGE = 'carrier_refunds_data_import'

REDSHIFT_CONN = 'redshift_default'
S3_BUCKET = 'grover-eu-central-1-production-data-bi-curated'
S3_DESTINATION_KEY = "google_sheets/operations/carrier_refunds_ops.json"

default_args = {'owner': 'bi',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 8, 8),
                'retries': 2,
                'retry_delay': datetime.timedelta(seconds=15),
                'retry_exponential_backoff': True,
                'on_failure_callback': on_failure_callback
                }

dag = DAG(
    dag_id="carrier_refunds_ops_data_import",
    default_args=default_args,
    schedule_interval='0 10 * * 1-5',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['gsheet', 'bi', 'ops', 'carrier_refunds'],
)

task_google_sheets_values_to_s3 = GoogleApiToS3Operator(
    dag=dag,
    google_api_service_name='sheets',
    google_api_service_version='v4',
    google_api_endpoint_path='sheets.spreadsheets.values.get',
    google_api_endpoint_params={'spreadsheetId': GOOGLE_SHEET_ID, 'range': GOOGLE_SHEET_RANGE},
    s3_destination_key=f's3://{S3_BUCKET}/{S3_DESTINATION_KEY}',
    task_id='google_sheets_to_s3',
    gcp_conn_id='google_cloud_default',
    s3_overwrite=True
)

create_table_task = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="create_table",
    sql="./sql/create_table.sql"
)

truncate_table_task = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="truncate_table",
    sql="./sql/truncate_table.sql"
)

task_load_data_to_redshift = PythonOperator(
    dag=dag,
    task_id='load_data_to_redshift',
    python_callable=load_gsheet_s3_to_redshift_in_append,
    provide_context=True,
    do_xcom_push=True,
    op_kwargs={"s3_bucket": S3_BUCKET,
               "s3_key": S3_DESTINATION_KEY,
               "target_table": "carrier_refunds_ops",
               "target_schema": "staging_google_sheet"}
)


task_google_sheets_values_to_s3 >> create_table_task\
      >> truncate_table_task >> task_load_data_to_redshift
