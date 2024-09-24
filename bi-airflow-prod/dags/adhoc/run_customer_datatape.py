import datetime
import logging

from airflow import DAG

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 8, 11),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=5)}

DAG_ID = 'customer_data_tape_from_repo'

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=get_schedule('0 11 * * SAT'),
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=15),
    tags=['Git', 'redshift'])

run_customer_datatape_sql = RunQueryFromRepoOperator(
    dag=dag,
    task_id='run_customer_datatape_sql',
    conn_id='redshift_default',
    directory='4_dwh_product',
    file='Customer Data Tape'
)

run_customer_datatape_sql
