import datetime as dt
import json
import logging
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

logger = logging.getLogger(__name__)

DAG_ID = 'pricing_load_pipeline'
REDSHIFT_CONN = 'redshift_default'

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': dt.datetime(2022, 11, 3),
    'retries': 2,
    'on_failure_callback': on_failure_callback
}

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

with open(os.path.join(__location__, 'pipeline_config.json'), 'r') as conf:
    config = json.loads(conf.read())

with DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval=get_schedule('21 9,17,23 * * *'),
        max_active_runs=1,
        catchup=False,
        dagrun_timeout=dt.timedelta(minutes=180),
        tags=[DAG_ID, 'pricing', 'pipeline']) as dag:
    t0 = EmptyOperator(task_id="begin", dag=dag)
    t1 = EmptyOperator(task_id="end", dag=dag)

    prev_task = None
    with TaskGroup('data_pricing', prefix_group_id=False) as data_pricing:
        group = config['data_pricing']
        for script in group['script_name']:
            sql_run = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
                repo='data-pricing'
            )
            if prev_task:
                prev_task >> sql_run
            prev_task = sql_run

    prev_task = None

run_pricing_snapshot_sql = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="pricing_snapshot",
    sql="./sql/pricing_snapshot.sql"
)

t0 >> data_pricing >> run_pricing_snapshot_sql >> t1
