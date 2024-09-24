import datetime
import logging

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from business_logic.braze.canvas_details import (
    extract_canvas_ids_to_s3, extract_raw_canvas_details_to_s3,
    write_curated_canvas_details_to_s3)
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'braze_canvas_details'
REDSHIFT_CONN = 'redshift'
EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '2G',
        'request_cpu': '1000m',
        'limit_memory': '4G',
        'limit_cpu': '2000m'
    }
}

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 5, 23),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=20)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Export Braze canvas data and load to S3 / Redshift',
    schedule_interval=get_schedule('0 7 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=['braze', 'redshift', 'api', DAG_ID]
)

extract_canvas_ids_to_s3 = ShortCircuitOperator(
        dag=dag,
        task_id='extract_canvas_ids_to_s3',
        python_callable=extract_canvas_ids_to_s3,
        executor_config=EXECUTOR_CONFIG
)

extract_raw_canvas_details_to_s3 = PythonOperator(
        dag=dag,
        task_id='extract_raw_canvas_details_to_s3',
        python_callable=extract_raw_canvas_details_to_s3,
        executor_config=EXECUTOR_CONFIG
        )

write_curated_canvas_details_to_s3 = PythonOperator(
        dag=dag,
        task_id='write_curated_canvas_details_to_s3',
        python_callable=write_curated_canvas_details_to_s3,
        executor_config=EXECUTOR_CONFIG
        )

stage_data = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="stage_data",
    sql="./sql/stage_data.sql"
)

create_target_table = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="create_target_table",
    sql="./sql/create_target_table.sql"
)

upsert_data = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="upsert_data",
    sql="./sql/upsert_data.sql",
    autocommit=False
)

clean_up = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="clean_up",
    sql="./sql/clean_up.sql",
    autocommit=True
)

chain(extract_canvas_ids_to_s3, extract_raw_canvas_details_to_s3,
      write_curated_canvas_details_to_s3, [stage_data, create_target_table],
      upsert_data, clean_up)
