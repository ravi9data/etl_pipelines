import datetime
import logging

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

DAG_ID = 'monitoring_grover_issues'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 12, 13),
                'retries': 3,
                'on_failure_callback': on_failure_callback}

config = {
    "grover_issues": {
        "directory": "8_monitoring/grover_issues",
        "script_name": [
            "b2b_or1", "oi1", "oi2", "oi3",
            "oo1", "oo2", "oo3", "oo4",  "or1", "or2", "or3",  "or4", "or5",
            "smc1", "smp temp", "smp1", "ot1"
        ]
    },
    "system_issues": {
        "directory": "8_monitoring/system_issues",
        "script_name": [
            "master_duplicates", "missing_dates"
        ]
    }
}

with DAG(DAG_ID,
         default_args=default_args,
         schedule_interval=get_schedule('51 7,11,16 * * *'),
         max_active_runs=1,
         catchup=False,
         tags=[DAG_ID, 'monitoring', 'grover_issues']) as dag:
    prev_task = None
    with TaskGroup('grover_issues', prefix_group_id=False) as grover_issues:
        group = config['grover_issues']
        for script in group['script_name']:
            run_sql = RunQueryFromRepoOperator(
                dag=dag,
                task_id=str(script).replace(' ', '_'),
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
            )
            if prev_task:
                prev_task >> run_sql
            prev_task = run_sql

    with TaskGroup('system_issues', prefix_group_id=False) as system_issues:
        group = config['system_issues']
        for script in group['script_name']:
            run_sql = RunQueryFromRepoOperator(
                dag=dag,
                task_id=str(script).replace(' ', '_'),
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
            )
            if prev_task:
                prev_task >> run_sql
            prev_task = run_sql
