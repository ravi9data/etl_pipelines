import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'hourly_live_reporting'
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
    description='Hourly live reporting',
    schedule_interval=get_schedule('*/30 7-22 * * *'),  # TODO refactor weekeend runs
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['live_reporting', 'hourly_live_reporting', DAG_ID]
)


staging_data_refresh = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="staging_data_refresh",
    sql="./sql/staging_data_refresh.sql"
)

bi_ods_order_decision = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="bi_ods_order_decision",
    sql="./sql/bi_ods_order_decision.sql"
)

bi_ods_order_fraud_check = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="bi_ods_order_fraud_check",
    sql="./sql/bi_ods_order_fraud_check.sql"
)

live_reporting_order = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="live_reporting_order",
    sql="./sql/live_reporting_order.sql"
)

live_reporting_order_item = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="live_reporting_order_item",
    sql="./sql/live_reporting_order_item.sql"
)

live_reporting_subscription = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="live_reporting_subscription",
    sql="./sql/live_reporting_subscription.sql"
)

staging_data_refresh >> bi_ods_order_decision >> bi_ods_order_fraud_check >> \
      live_reporting_order >> live_reporting_order_item >> live_reporting_subscription
