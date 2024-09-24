import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'daily_live_reporting'
REDSHIFT_CONN = 'redshift_default'


default_args = {
    'owner': 'bi',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 11, 8),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='daily live reporting',
    schedule_interval=get_schedule('0 9 * * *'),
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['live_reporting', 'daily_live_reporting', DAG_ID]
)


staging_data_refresh = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="staging_data_refresh",
    sql="./sql/staging_data_refresh.sql"
)

bi_ods_product = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="bi_ods_product",
    sql="./sql/bi_ods_product.sql"
)

bi_ods_store = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="bi_ods_store",
    sql="./sql/bi_ods_store.sql"
)

bi_ods_variant = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="bi_ods_variant",
    sql="./sql/bi_ods_variant.sql"
)

live_reporting_rental_plans = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="live_reporting_rental_plans",
    sql="./sql/live_reporting_rental_plans.sql"
)

live_reporting_billing_payments_final = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="live_reporting_billing_payments_final",
    sql="./sql/live_reporting_billing_payments_final.sql"
)

staging_data_refresh >> bi_ods_product >> bi_ods_store >> bi_ods_variant >> \
    live_reporting_rental_plans >> live_reporting_billing_payments_final
