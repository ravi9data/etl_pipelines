import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from business_logic.ixopay.transactions_extract import extract_data_to_s3
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'ixopay_transactions_extract'
REDSHIFT_CONN = 'redshift'
EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '500M',
        'request_cpu': '500m',
        'limit_memory': '2G',
        'limit_cpu': '2000m'
    }
}

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 4, 4),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=120),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=10)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Extract Ixopay transactions data and upsert to Redshift',
    schedule_interval=get_schedule('10 */2 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=['ixopay', 'redshift', 'api', DAG_ID]
)

extract_data_to_s3 = PythonOperator(
        dag=dag,
        task_id='extract_data_to_s3',
        python_callable=extract_data_to_s3,
        executor_config=EXECUTOR_CONFIG
        )

stage_transactions = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="stage_transactions",
    sql="./sql/stage_transactions.sql"
)

upsert_transactions = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="upsert_transactions",
    sql="./sql/upsert_transactions.sql",
    autocommit=False
)

clean_up = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="clean_up",
    sql="./sql/clean_up.sql",
    autocommit=False
)

extract_data_to_s3 >> stage_transactions >> upsert_transactions >> clean_up
