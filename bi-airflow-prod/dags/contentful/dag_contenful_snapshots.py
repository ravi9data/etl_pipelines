import datetime as dt
import logging

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from dags.contentful.fetch_snapshots import cf_snapshot_to_redshift
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'contenful_snapshots'

logger = logging.getLogger(__name__)

EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '500Mi',
        'request_cpu': '500m',
        'limit_memory': '6G',
        'limit_cpu': '2000m'
    }
}

extract_date = dt.datetime.now()

default_args = {
    'owner': 'bi',
    'depends_on_past': False,
    'start_date': dt.datetime(2022, 10, 12),
    'retries': 2,
    'retry_delay': dt.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': dt.timedelta(minutes=60),
    'params': {'extract_date': extract_date}
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Get snapshots from CF',
    schedule_interval=get_schedule('0 23 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=['Automation', 'pricing']
)

write_snapshot_to_redshift = ShortCircuitOperator(
    dag=dag,
    task_id='cf_snapshot_to_redshift',
    python_callable=cf_snapshot_to_redshift,
    executor_config=EXECUTOR_CONFIG,
    op_kwargs={'extract_date': '{{ params.extract_date }}'}
)

upsert = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="upsert",
    sql="./sql/cf_upsert.sql",
    autocommit=True
)

write_snapshot_to_redshift >> upsert
