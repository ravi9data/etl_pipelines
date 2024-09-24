import datetime as dt
import json
import logging
import os

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

logger = logging.getLogger(__name__)

DAG_ID = 'braze_data_mart_refresh'

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': dt.datetime(2022, 10, 4),
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
        schedule_interval=get_schedule('25 7 * * *'),
        max_active_runs=1,
        catchup=False,
        dagrun_timeout=dt.timedelta(minutes=120),
        tags=[DAG_ID, 'braze', 'marketing']) as dag:
    prev_task = None
    with TaskGroup('braze_dm', prefix_group_id=False) as braze_dm:
        group = config['braze_dm']
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
