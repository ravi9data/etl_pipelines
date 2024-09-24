import datetime as dt
import logging

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

logger = logging.getLogger(__name__)

DAG_ID = 'store_specific_ranking'
REDSHIFT_CONN = 'redshift_default'

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': dt.datetime(2022, 11, 3),
    'retries': 2,
    'on_failure_callback': on_failure_callback
}

config = {
    "90_Hightouch": {
        "directory": "90_Hightouch",
        "script_name": [
            "Store_ranking",
            "performance_tracker",
            "product_data_livefeed",
            "subs_cancelled",
            "pending_allocations"
        ]
    }
}

with DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval=get_schedule('8 8,11,19 * * *'),
        max_active_runs=1,
        catchup=False,
        dagrun_timeout=dt.timedelta(minutes=180),
        tags=[DAG_ID, 'pricing', 'pipeline']) as dag:
    prev_task = None
    with TaskGroup('hightouch', prefix_group_id=False) as hightouch:
        group = config['90_Hightouch']
        for script in group['script_name']:
            sql_run = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script
            )
            if prev_task:
                prev_task >> sql_run
            prev_task = sql_run

    prev_task = None
