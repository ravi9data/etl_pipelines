import datetime
import logging

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.utils.task_group import TaskGroup

from dags.sf_b2b_hourly.config import b2b_pipeline_config
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

DAG_ID = 'sf_b2b_hourly_refresh'

logger = logging.getLogger(__name__)

config = b2b_pipeline_config

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 3,
                'on_failure_callback': on_failure_callback}

with DAG(DAG_ID,
         default_args=default_args,
         description='DAG to fetch exchange rate from API',
         schedule_interval=get_schedule('0 9,17 * * *'),
         max_active_runs=1,
         catchup=False,
         tags=[DAG_ID, 'salesforce', 'b2b']) as dag:
    stage_sf_b2b_data = AirbyteTriggerSyncOperator(
        task_id='airbyte_stage_sf_b2b_data',
        airbyte_conn_id='airbyte_prod',
        connection_id='b1c1165d-5ca3-4694-bb06-fe0636fd4a1e',
        asynchronous=True
    )

    prev_task = None

    with TaskGroup('sf_b2b_sql', prefix_group_id=False) as run_git_scripts:
        group = config['sf_b2b']
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

    stage_sf_b2b_data >> run_git_scripts
