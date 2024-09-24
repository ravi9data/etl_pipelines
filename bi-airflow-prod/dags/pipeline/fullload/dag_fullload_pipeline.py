import datetime as dt
import json
import logging
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator
from plugins.slack_utils import send_notification

logger = logging.getLogger(__name__)

DAG_ID = 'fullload_pipeline'

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': dt.datetime(2022, 11, 4),
    'retries': 0,
    'on_failure_callback': on_failure_callback,
    'retry_delay': dt.timedelta(seconds=60)
}

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


def slack_message_dataload_notifications():
    """
    Send pipeline completion status message to #dataload_notifications channel

    """
    report_date = dt.datetime.today().strftime('%Y-%m-%d')
    message = f"*FullLoad Pipeline:* \nExecution completed for reporting date: *{report_date}*"
    send_notification(message=message)


with open(os.path.join(__location__, 'pipeline_config.json'), 'r') as conf:
    config = json.loads(conf.read())

with DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval=get_schedule('30 0 * * *'),
        max_active_runs=1,
        catchup=False,
        tags=[DAG_ID, 'fullload', 'pipeline']) as dag:

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
            for script in group['script_name']:
                run_sql = RunQueryFromRepoOperator(
                    dag=dag,
                    task_id=script,
                    conn_id='redshift_default',
                    directory=group['directory'],
                    file=script,
                    trigger_rule='all_done',
                    retries=2
                )
                if prev_task:
                    prev_task >> run_sql
                prev_task = run_sql

    send_status = PythonOperator(
        task_id="send_slack_notification",
        python_callable=slack_message_dataload_notifications,
        dag=dag
    )

    tg >> send_status
