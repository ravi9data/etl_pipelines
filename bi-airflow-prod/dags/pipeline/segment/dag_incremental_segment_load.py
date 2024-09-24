import datetime as dt

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from pipeline.segment.config import config

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

DAG_ID = 'incremental_segment_load'
REDSHIFT_CONN = 'redshift_default'

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': dt.datetime(2023, 4, 6),
                'retries': 3,
                'retry_delay': dt.timedelta(seconds=60),
                'on_failure_callback': on_failure_callback}

with DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval=get_schedule('30 1 * * *'),
        max_active_runs=1,
        catchup=False,
        dagrun_timeout=dt.timedelta(minutes=180),
        tags=[DAG_ID, 'segment']) as dag:
    t0 = EmptyOperator(task_id="begin", dag=dag)
    t1 = EmptyOperator(task_id="end", dag=dag)

    prev_task = None
    with TaskGroup('30_segment_events', prefix_group_id=False) as segment_events:
        group = config['segment_events']
        for script in group['script_name']:
            snowplow_run = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
            )
            if prev_task:
                prev_task >> snowplow_run
            prev_task = snowplow_run

    prev_task = None
    with TaskGroup('31_segment_page_view_and_sessions', prefix_group_id=False) as segment_page_view_and_sessions:   # noqa: E501
        group = config['segment_page_view_and_sessions']
        for script in group['script_name']:
            snowplow_run = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
            )
            if prev_task:
                prev_task >> snowplow_run
            prev_task = snowplow_run

        chain(
            t0,
            segment_events,
            segment_page_view_and_sessions,
            t1
        )
