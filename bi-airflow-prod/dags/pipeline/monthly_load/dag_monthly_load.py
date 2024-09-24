import datetime as dt
import logging

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from dags.pipeline.monthly_load.config import monthly_load_pipeline_config
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

logger = logging.getLogger(__name__)

DAG_ID = 'monthly_load_v1'

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': dt.datetime(2022, 11, 4),
    'retries': 0,
    'on_failure_callback': on_failure_callback,
    'retry_delay': dt.timedelta(seconds=60)
}

with DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval=get_schedule('30 9 1 * *'),
        max_active_runs=1,
        catchup=False,
        tags=[DAG_ID, 'pipeline', 'finance', 'risk', 'commercial']) as dag:
    prev_task = None
    with TaskGroup('dwh_finance_monthly', prefix_group_id=False) as dwh_finance_monthly:
        group = monthly_load_pipeline_config['dwh_finance_monthly']
        for script in group['script_name']:
            if script != 'earned_revenue_historical':
                run_sql = RunQueryFromRepoOperator(
                    dag=dag,
                    task_id=script,
                    conn_id='redshift_default',
                    directory=group['directory'],
                    file=script,
                    trigger_rule='all_done'
                )
                if prev_task:
                    prev_task >> run_sql
                prev_task = run_sql

    prev_task = None
    with TaskGroup('dwh_dc_monthly', prefix_group_id=False) as dwh_dc_monthly:
        group = monthly_load_pipeline_config['dwh_dc_monthly']
        for script in group['script_name']:
            run_sql = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
                trigger_rule='all_done'
            )
            if prev_task:
                prev_task >> run_sql
            prev_task = run_sql

    dwh_finance_monthly >> dwh_dc_monthly
