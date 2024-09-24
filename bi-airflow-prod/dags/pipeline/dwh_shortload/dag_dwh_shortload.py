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

DAG_ID = 'dwh_shortload_pipeline'

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
        schedule_interval=get_schedule('0 10,12,16 * * 1-5,7'),
        max_active_runs=1,
        catchup=False,
        dagrun_timeout=dt.timedelta(minutes=180),
        tags=[DAG_ID, 'dwh', 'pipeline']) as dag:
    t0 = EmptyOperator(task_id="begin", dag=dag)
    t1 = EmptyOperator(task_id="end", dag=dag)

    prev_task = None
    with TaskGroup('4_dwh_bd', prefix_group_id=False) as dwh_bd:
        group = config['4_dwh_bd']
        for script in group['script_name']:
            sql_run = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
                trigger_rule='all_done',
                autocommit=False
            )
            if prev_task:
                prev_task >> sql_run
            prev_task = sql_run

    prev_task = None
    with TaskGroup('4_dwh_catman', prefix_group_id=False) as dwh_catman:
        group = config['4_dwh_catman']
        for script in group['script_name']:
            sql_run = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
                trigger_rule='all_done',
                autocommit=False
            )
            if prev_task:
                prev_task >> sql_run
            prev_task = sql_run

    prev_task = None
    with TaskGroup('4_dwh_core', prefix_group_id=False) as dwh_core:
        group = config['4_dwh_core']
        for script in group['script_name']:
            sql_run = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
                trigger_rule='all_done',
                autocommit=False
            )
            if prev_task:
                prev_task >> sql_run
            prev_task = sql_run

    prev_task = None
    with TaskGroup('4_dwh_marketing', prefix_group_id=False) as dwh_marketing:
        group = config['4_dwh_marketing']
        for script in group['script_name']:
            sql_run = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
                trigger_rule='all_done',
                autocommit=False
            )
            if prev_task:
                prev_task >> sql_run
            prev_task = sql_run

    prev_task = None
    with TaskGroup('4_dwh_ops', prefix_group_id=False) as dwh_ops:
        group = config['4_dwh_ops']
        for script in group['script_name']:
            sql_run = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
                trigger_rule='all_done',
                autocommit=False
            )
            if prev_task:
                prev_task >> sql_run
            prev_task = sql_run

    prev_task = None
    with TaskGroup('4_dwh_risk', prefix_group_id=False) as dwh_risk:
        group = config['4_dwh_risk']
        for script in group['script_name']:
            sql_run = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
                trigger_rule='all_done',
                autocommit=False
            )
            if prev_task:
                prev_task >> sql_run
            prev_task = sql_run
    prev_task = None

    with TaskGroup('4_dwh_product', prefix_group_id=False) as dwh_product:
        group = config['4_dwh_product']
        for script in group['script_name']:
            sql_run = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
                trigger_rule='all_done',
                autocommit=False
            )
            if prev_task:
                prev_task >> sql_run
            prev_task = sql_run

    with TaskGroup('11_marketing_reporting', prefix_group_id=False) as dwh_marketing_reporting:
        group = config['11_marketing_reporting']
        for script in group['script_name']:
            sql_run = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
                trigger_rule='all_done',
                autocommit=False
            )
            if prev_task:
                prev_task >> sql_run
            prev_task = sql_run

        chain(
            t0,
            dwh_bd,
            dwh_catman,
            dwh_core,
            dwh_marketing,
            dwh_ops,
            dwh_risk,
            dwh_product,
            dwh_marketing_reporting,
            t1
        )
