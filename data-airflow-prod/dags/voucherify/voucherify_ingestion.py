import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from business_logic.voucherify.create_bulk_export import (
    get_columns_to_keep, ingest_from_export_api)
from business_logic.voucherify.validation_rules_api import \
    ingest_api_validation_rules
from business_logic.voucherify.validation_rules_extraction import \
    validation_rules_extraction_to_s3
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.redshift.redshift_copy_operator import \
    RedshiftCopyOperator

DAG_ID = 'voucherify_integration_v3'
REDSHIFT_CONN = 'redshift'

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 8, 15, 12, 0),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=45),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=25)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Ingest Voucherify API data into S3 and Redshift',
    schedule_interval=get_schedule('0 0 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID, "ingestion", "data-eng", "voucherify"]
)

get_vouchers_export = PythonOperator(
    dag=dag,
    task_id='get_vouchers_export',
    python_callable=ingest_from_export_api,
    op_kwargs={"entity": "voucher"}
)

get_redemption_export = PythonOperator(
    dag=dag,
    task_id='get_redemption_export',
    python_callable=ingest_from_export_api,
    op_kwargs={"entity": "redemption"}
)

get_validation_rules = PythonOperator(
    dag=dag,
    task_id='get_validation_rules',
    python_callable=ingest_api_validation_rules
)

validation_rules_extraction_s3 = PythonOperator(
    dag=dag,
    task_id='validation_rules_extraction_s3',
    python_callable=validation_rules_extraction_to_s3
)

truncate_staging_tables = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="truncate_staging_tables",
    sql="./sql/truncate_staging_tables.sql"
)

copy_validation_rules_task = RedshiftCopyOperator(
    dag=dag,
    task_id='copy_validation_rules_to_redshift',
    redshift_conn_id=REDSHIFT_CONN,
    iam_role_arn='{{ var.value.redshift_iam_role_arn }}',
    schema_target='staging',
    table_target='voucherify_validation_rules',
    columns=['id',
             'name',
             'rules',
             'applicable_to',
             'created_at',
             'type',
             'context_type',
             'assignments_count',
             'object'
             ],
    s3_location="{{ ti.xcom_pull(task_ids='get_validation_rules',\
        key='s3_file_path_validation_rules') }}",
    delimiter=','
)

copy_validation_rules_extraction_task = RedshiftCopyOperator(
    dag=dag,
    task_id='copy_validation_rules_extraction_to_redshift',
    redshift_conn_id=REDSHIFT_CONN,
    iam_role_arn='{{ var.value.redshift_iam_role_arn }}',
    schema_target='staging',
    table_target='voucherify_validation_rules_extracted',
    columns=['val_rule_id',
             'val_rule_name',
             'rules',
             'created_at',
             'type',
             'context_type',
             'assignments_count',
             'object',
             'applicable_to',
             'rule_id',
             'name',
             'property',
             'conditions'
             ],
    s3_location="{{ ti.xcom_pull(task_ids='validation_rules_extraction_s3',\
        key='s3_file_path_validation_rules_extraction') }}",
    delimiter=','
)

copy_redemption_task = RedshiftCopyOperator(
    dag=dag,
    task_id='copy_redemption_to_redshift',
    redshift_conn_id=REDSHIFT_CONN,
    iam_role_arn='{{ var.value.redshift_iam_role_arn }}',
    schema_target='staging',
    table_target='voucherify_redemption',
    columns=get_columns_to_keep('redemption'),
    s3_location="{{ ti.xcom_pull(task_ids='get_redemption_export',\
        key='s3_file_path_redemption') }}",
    delimiter=','
)

copy_voucher_task = RedshiftCopyOperator(
    dag=dag,
    task_id='copy_vouchers_to_redshift',
    redshift_conn_id=REDSHIFT_CONN,
    iam_role_arn='{{ var.value.redshift_iam_role_arn }}',
    schema_target='staging',
    table_target='voucherify_voucher',
    columns=get_columns_to_keep('voucher'),
    s3_location="{{ ti.xcom_pull(task_ids='get_vouchers_export', key='s3_file_path_voucher') }}",
    delimiter=','
)

upsert_validation_rules = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="upsert_validation_rules",
    sql="./sql/upsert_validation_rules.sql"
)

upsert_validation_rules_extraction = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="upsert_validation_rules_extraction",
    sql="./sql/upsert_validation_rules_extraction.sql"
)

upsert_redemptions = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="upsert_redemptions",
    sql="./sql/upsert_redemptions.sql"
)

upsert_vouchers = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="upsert_vouchers",
    sql="./sql/upsert_vouchers.sql"
)

insert_voucherify_voucher_transactions = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="insert_voucherify_voucher_transactions",
    sql="./sql/insert_voucherify_voucher_transactions.sql"
)

insert_voucherify_voucher_enriched = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="insert_voucherify_voucher_enriched",
    sql="./sql/insert_voucherify_voucher_enriched.sql"
)

get_validation_rules >> validation_rules_extraction_s3 >> get_redemption_export \
    >> get_vouchers_export >> truncate_staging_tables >> [
        copy_validation_rules_task,
        copy_validation_rules_extraction_task,
        copy_redemption_task,
        copy_voucher_task
    ]
copy_validation_rules_task >> upsert_validation_rules
copy_validation_rules_extraction_task >> upsert_validation_rules_extraction
copy_redemption_task >> upsert_redemptions
copy_voucher_task >> upsert_vouchers

upsert_validation_rules >> insert_voucherify_voucher_transactions \
    >> insert_voucherify_voucher_enriched
upsert_validation_rules_extraction >> insert_voucherify_voucher_transactions \
    >> insert_voucherify_voucher_enriched
upsert_redemptions >> insert_voucherify_voucher_transactions \
    >> insert_voucherify_voucher_enriched
upsert_vouchers >> insert_voucherify_voucher_transactions \
    >> insert_voucherify_voucher_enriched
