import datetime as dt
import json
import logging
import os

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

from plugins.dag_utils import (get_schedule, on_failure_callback,
                               on_sla_miss_callback)
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator
from plugins.operators.SodaChecksOperator import SodaChecksOperator

logger = logging.getLogger(__name__)

DAG_ID = 'shortload_pipeline'

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': dt.datetime(2022, 11, 4),
    'retries': 2,
    'sla': dt.timedelta(minutes=10),
    'on_failure_callback': on_failure_callback,
    'sla_miss_callback': on_sla_miss_callback,
    'retry_delay': dt.timedelta(seconds=60)
}

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

with open(os.path.join(__location__, 'pipeline_config.json'), 'r') as conf:
    config = json.loads(conf.read())

with DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval=get_schedule('0 12,20 * * *'),
        max_active_runs=1,
        catchup=False,
        tags=[DAG_ID, 'pipeline']
        ) as dag:
    staging_refresh = SQLExecuteQueryOperator(
        dag=dag,
        task_id='staging_refresh',
        conn_id='redshift_default',
        sql='./sql/staging_data_refresh.sql',
        autocommit=False,
        split_statements=True
    )

    prev_task = staging_refresh

    for task_group in config:
        with TaskGroup(task_group, prefix_group_id=False) as tg:
            group = config[task_group]
            for script in group['scripts_list']:
                run_sql = RunQueryFromRepoOperator(
                    dag=dag,
                    task_id=script['script_name'],
                    conn_id='redshift_default',
                    directory=group['directory'],
                    file=script['script_name'],
                    trigger_rule='all_done',
                    retries=2
                )

                if script.get('check_file', None):
                    base_path = os.path.join(__location__,
                                             '../../../business_logic/data_quality/checks')
                    check_file_name = script['check_file']
                    schema_name = script['schema']
                    check_file_location = os.path.join(base_path, schema_name,
                                                       check_file_name + '.yaml')
                    run_checks = SodaChecksOperator(
                        task_id=f'{schema_name}_{check_file_name}_data_checks',
                        dag=dag,
                        soda_checks_path=check_file_location,
                        schema_name=schema_name,
                        trigger_rule='all_done',
                        log_results=True
                    )
                    run_sql >> run_checks

                if prev_task:
                    prev_task >> run_sql
                prev_task = run_sql
