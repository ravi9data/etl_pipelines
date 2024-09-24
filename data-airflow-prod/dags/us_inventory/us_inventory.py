import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from business_logic.us_inventory_data import (copy_to_s3,
                                              get_datetime_from_filename,
                                              get_latest_report_file)
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.redshift.redshift_copy_operator import \
    RedshiftCopyOperator

DAG_ID = 'us_inventory_data'
REDSHIFT_CONN = 'redshift'

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 11, 15),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=5),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=10)
}


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='get latest us inventory file in the UPS bucket and move to DENG bucket',
    schedule_interval=get_schedule('13 8,18 * * 1-6'),
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID]
)


get_latest_report_file_task = PythonOperator(
    dag=dag,
    task_id='get_latest_report_file',
    python_callable=get_latest_report_file
)


copy_to_s3_task = PythonOperator(
    dag=dag,
    task_id='copy_to_s3',
    python_callable=copy_to_s3
)


create_staging_table_task = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="create_staging_table",
    sql="./sql/staging_table.sql"
)


create_final_table_task = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="create_final_table",
    sql="./sql/final_table.sql"
)


copy_data_task = RedshiftCopyOperator(
    dag=dag,
    task_id='copy_data',
    redshift_conn_id=REDSHIFT_CONN,
    iam_role_arn='{{ var.value.redshift_iam_role_arn }}',
    schema_target='stg_external_apis_dl',
    table_target='us_inventory',
    columns=[
        "account",
        "item_number",
        "item_description",
        "warehouse",
        "location",
        "onhand_qty",
        "reserved_qty",
        "available_qty",
        "vendor_serial",
        "vendor_lot",
        "lpn",
        "designator",
        "revision",
        "expiration_date",
        "hazard_class",
        "cost",
        "date_received_gmt",
        "invoice_po_no",
        "rma_no",
        "supplier_vendor",
        "location_plant_id",
        "country_territory_of_origin",
        "part_condition_type",
        "po_line_order_line",
        "date_of_repair",
        "date_of_manufacture",
        "storage_location",
        "fiscal_note",
        "rec_ref_1",
        "rec_ref_2",
        "rec_ref_3",
        "rec_ref_4",
        "rec_ref_5",
        "rec_ref_6",
        "rec_ref_7",
        "rec_ref_8",
        "rec_ref_9",
        "rec_ref_10",
        "rec_ref_11",
        "rec_ref_12",
        "rec_ref_13",
        "rec_ref_14",
        "rec_ref_15",
        "rec_ref_16",
        "rec_ref_17",
        "rec_ref_18",
        "rec_ref_19",
        "rec_ref_20",
        "attribute_1",
        "attribute_2",
        "attribute_3",
        "attribute_4",
        "attribute_5",
        "attribute_6",
        "attribute_7",
        "attribute_8",
        "attribute_9",
        "attribute_10",
        "attribute_11",
        "attribute_12",
        "attribute_13",
        "attribute_14",
        "attribute_15",
        "attribute_16",
        "attribute_17",
        "attribute_18",
        "attribute_19",
        "attribute_20"
        ],
    s3_location="{{ ti.xcom_pull(task_ids='copy_to_s3')}}",
    delimiter=','
)


get_datetime_from_filename_task = PythonOperator(
    dag=dag,
    task_id='date_time_extraction_from_filename',
    python_callable=get_datetime_from_filename
)


upsert_task = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="upsert",
    sql="./sql/upsert.sql"
)


cleanup_task = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="cleanup",
    sql="./sql/cleanup.sql"
)

get_latest_report_file_task >> [copy_to_s3_task, get_datetime_from_filename_task,
                                create_final_table_task, create_staging_table_task]\
                             >> copy_data_task >> upsert_task >> cleanup_task
