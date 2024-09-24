import datetime as dt
import json
import logging
import os

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

logger = logging.getLogger(__name__)

DAG_ID = 'supply_chain_dasboard_dwh'

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': dt.datetime(2022, 10, 4),
    'retries': 3,
    'on_failure_callback': on_failure_callback
}

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

with open(os.path.join(__location__, 'supply_chain_config.json'), 'r') as conf:
    config = json.loads(conf.read())

with DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval=get_schedule('5 7,10,11,18 * * *'),
        max_active_runs=1,
        catchup=False,
        dagrun_timeout=dt.timedelta(minutes=240),
        tags=[DAG_ID, 'supply_chain_dasboard_dwh']) as dag:
    t0 = EmptyOperator(task_id="begin", dag=dag)
    t1 = EmptyOperator(task_id="end", dag=dag)

    prev_task = None

    with TaskGroup('da2_4_dwh', prefix_group_id=False) as dwh_ops:
        group = config['da2_4_dwh']
        for script in group['script_name']:
            supply_chain_run = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
            )
            if prev_task:
                prev_task >> supply_chain_run
            prev_task = supply_chain_run

    prev_task = None

    with TaskGroup('8_monitoring', prefix_group_id=False) as monitoring:
        group = config['monitoring']
        for script in group['script_name']:
            monitoring_run = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
            )
            if prev_task:
                prev_task >> monitoring_run
            prev_task = monitoring_run

        chain(
            t0,
            dwh_ops,
            monitoring,
            t1
        )
