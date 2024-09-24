import datetime
import logging

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from dags.pipeline.loyalty.config import loyalty_pipeline_config
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

DAG_ID = 'loyalty_pipeline'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 12, 13),
                'retries': 3,
                'on_failure_callback': on_failure_callback}

config = loyalty_pipeline_config

with DAG(DAG_ID,
         default_args=default_args,
         schedule_interval=get_schedule('45 6,17 * * *'),
         max_active_runs=1,
         catchup=False,
         tags=[DAG_ID, 'grover_card', 'loyalty']) as dag:
    prev_task = None
    with TaskGroup('loyalty', prefix_group_id=False) as loyalty:
        group = config['loyalty']
        for script in group['script_name']:
            run_sql = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
            )
            if prev_task:
                prev_task >> run_sql
            prev_task = run_sql
