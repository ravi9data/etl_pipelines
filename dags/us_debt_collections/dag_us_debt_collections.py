import datetime as dt
import logging
import os

import sqlalchemy
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.SodaChecksOperator import SodaChecksOperator
from plugins.utils.gsheet import export_to_gsheet, load_data_from_gsheet

DAG_ID = "us_debt_collections"
REDSHIFT_CONN = "redshift_default"
GOOGLE_CLOUD_CONN = "google_cloud_default"

STAGING_SOURCE_SHEET_ID = "14iwl00zI_sKDF3sD3fFL8dz7HM0SytGJAz7H6Tp6GTE"
STAGING_DESTINATION_SHEET_ID = "1cVl6TTCFRuPU4hpyVRkjPPwe8Nm2LRtuYObIEwXvrr0"
SOURCE_SHEET_RANGE_DEFAULT = "DC_notes + sub_id"
DESTINATION_SHEET_RANGE_DEFAULT = "BI RAW"

SOURCE_SHEET_ID_VAR = "us_debt_source_gsheet_id"
DESTINATION_SHEET_ID_VAR = "us_debt_destination_gsheet_id"
SOURCE_SHEET_RANGE_VAR = "us_debt_source_range"
DESTINATION_SHEET_RANGE_VAR = "us_debt_destination_range"

logger = logging.getLogger(__name__)

EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '500Mi',
        'request_cpu': '500m',
        'limit_memory': '5G',
        'limit_cpu': '1500m'
    }
}

default_args = {"owner": "bi-eng", "depends_on_past": False,
                "start_date": dt.datetime(2022, 12, 7),
                "retries": 2,
                "retry_delay": dt.timedelta(seconds=5),
                "retry_exponential_backoff": True,
                "on_failure_callback": on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          description="Upload DC payments data to google sheets daily",
          schedule_interval=get_schedule("55 11 * * *"),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, "debt collection", "payments"])

extract_us_dc_data = PythonOperator(
    dag=dag,
    task_id="extract_us_dc_data",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": Variable.get(SOURCE_SHEET_ID_VAR, STAGING_SOURCE_SHEET_ID),
               "range_name": Variable.get(SOURCE_SHEET_RANGE_VAR, SOURCE_SHEET_RANGE_DEFAULT),
               "target_table": "us_dc_customer_contact",
               "target_schema": "debt_collection",
               "data_types": {"subscription_id": sqlalchemy.types.VARCHAR(length=65535),
                              "owner": sqlalchemy.types.VARCHAR(length=65535),
                              "date": sqlalchemy.types.VARCHAR(length=65535),
                              "channel": sqlalchemy.types.VARCHAR(length=65535),
                              "responded": sqlalchemy.types.VARCHAR(length=65535),
                              "current_state": sqlalchemy.types.VARCHAR(length=65535),
                              "team_notes": sqlalchemy.types.VARCHAR(length=65535),
                              "follow_up_date": sqlalchemy.types.VARCHAR(length=65535)
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

last_payment_event = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="last_payment_event",
    sql="./sql/last_payment_event.sql"
)

billing_service_updated_dpd = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="billing_service_updated_dpd",
    sql="./sql/billing_service_updated_dpd.sql"
)

detailed_view_us_dc_tmp = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="detailed_view_us_dc_tmp",
    sql="./sql/detailed_view_us_dc_tmp.sql"
)

detailed_view_us_dc = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="detailed_view_us_dc",
    sql="./sql/detailed_view_us_dc.sql"
)

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

base_path = os.path.join(__location__, '../../business_logic/data_quality/checks')
check_file_name = 'detailed_view_us_dc'
schema_name = 'ods_production'
check_file_location = os.path.join(base_path, schema_name,
                                   check_file_name + '.yaml')
run_checks_ods = SodaChecksOperator(
    task_id='data_quality_checks_detailed_view_us_dc',
    dag=dag,
    soda_checks_path=check_file_location,
    schema_name=schema_name,
    log_results=True,
    retries=1
)

check_file_name = 'us_dc_customer_contact'
schema_name = 'debt_collection'
check_file_location = os.path.join(base_path, schema_name,
                                   check_file_name + '.yaml')

run_checks_staging = SodaChecksOperator(
    task_id='data_quality_checks_us_dc_customer_contact',
    dag=dag,
    soda_checks_path=check_file_location,
    schema_name=schema_name,
    log_results=True,
    retries=1
)

export_sql = """
    SELECT
        order_id,
        subscription_id,
        customer_id,
        due_date,
        due_date_new,
        dpd,
        dpd_new,
        subscription_value,
        tax_amount,
        outstanding_subscription_revenue,
        first_name,
        last_name,
        email,
        phone_number,
        billing_city,
        billing_zip,
        billing_street,
        billing_state,
        shipping_city,
        shipping_zip,
        shipping_street,
        shipping_state,
        order_status,
        contract_status,
        subscription_plan,
        outstanding_products,
        serial_number,
        total_nr_payments,
        paid_payments,
        customer_type,
        subscription_start_date,
        payment_method,
        "asset_value_linear_depr (3% discount)",
        person_business,
        last_failed_reason,
        last_failed_date,
        birthdate,
        last_payment_date,
        first_asset_delivery_date,
        day_default_date,
        outstanding_rrp,
        outstanding_asset_value,
        outstanding_residual_asset_value,
        remaining_purchase_price_excl_sales_tax,
        remaining_purchase_price_incl_sales_tax,
        total_paid_amount_excl_sales_tax,
        total_paid_amount_incl_sales_tax,
        start_purchase_price_excl_sales_tax,
        start_purchase_price_incl_sales_tax
    FROM ods_production.detailed_view_us_dc;"""

export_us_dc_data = PythonOperator(
    dag=dag,
    task_id="export_us_dc_data",
    python_callable=export_to_gsheet,
    op_kwargs={
        "conn_id": GOOGLE_CLOUD_CONN,
        "sheet_id": Variable.get(DESTINATION_SHEET_ID_VAR, STAGING_DESTINATION_SHEET_ID),
        "range_name": Variable.get(DESTINATION_SHEET_RANGE_VAR, DESTINATION_SHEET_RANGE_DEFAULT),
        "sql": export_sql,
        "include_header": True,
        "sort_by": (7, 'des')
    },
    executor_config=EXECUTOR_CONFIG
)

extract_us_dc_data >> last_payment_event >> billing_service_updated_dpd \
    >> detailed_view_us_dc_tmp >> detailed_view_us_dc >> run_checks_ods >> run_checks_staging \
    >> export_us_dc_data
