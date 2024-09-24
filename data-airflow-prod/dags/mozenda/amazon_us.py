import datetime
import logging
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from business_logic.mozenda.amazon import \
    get_latest_data_available_for_amazon_us
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.redshift.redshift_copy_operator import \
    RedshiftCopyOperator

DAG_ID = 'mozenda_prices_crawler_amazon_us'
PARENT_FOLDER = os.path.basename(os.getcwd())
REDSHIFT_CONN = 'redshift'

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 6, 9),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=5),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=5)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Gets data available from Mozenda price-collection-us-output-amazon \
                , stored in S3, and writes it to Redshift.',
    schedule_interval=get_schedule('30 5 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=[PARENT_FOLDER, 'mozenda', 'amazon_us', 'prices']
)

get_latest_data_available_for_amazon_task = PythonOperator(
    dag=dag,
    task_id='get_latest_data_available_for_amazon_us',
    python_callable=get_latest_data_available_for_amazon_us
)

create_task = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id='create',
    sql='./sql/amazon_us/0_create.sql'
)

copy_data_task = RedshiftCopyOperator(
    dag=dag,
    task_id='copy_data',
    redshift_conn_id=REDSHIFT_CONN,
    iam_role_arn='{{ var.value.redshift_iam_role_arn }}',
    schema_target='staging_price_collection',
    table_target='amazon_us_copy_s3',
    columns=[
        "item_id",
        "product_name",
        "brand",
        "product_sku",
        "amazon_name",
        "main_ad_condition",
        "main_ad_price",
        "condition",
        "price",
        "crawled_at"],
    s3_location="{{ ti.xcom_pull(task_ids='get_latest_data_available_for_amazon_us') }}",
    delimiter=','
)

upsert_task = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id='upsert',
    sql='./sql/amazon_us/1_upsert.sql'
)

get_latest_data_available_for_amazon_task >> create_task >> copy_data_task \
    >> upsert_task
