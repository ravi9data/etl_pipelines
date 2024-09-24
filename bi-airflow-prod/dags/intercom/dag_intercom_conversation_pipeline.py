import datetime as dt
import logging

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

from dags.intercom.config import intercom_conversation_pipeline_config
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

logger = logging.getLogger(__name__)

DAG_ID = 'intercom_conversation_pipeline'

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': dt.datetime(2022, 10, 4),
    'retries': 3,
    'on_failure_callback': on_failure_callback
}

config = intercom_conversation_pipeline_config

with DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval=get_schedule('0 5,17,22 * * *'),
        max_active_runs=1,
        catchup=False,
        dagrun_timeout=dt.timedelta(minutes=180),
        tags=[DAG_ID, 'intercom', 'cs']) as dag:
    intercom_deleted_conversations = PostgresOperator(
        dag=dag,
        postgres_conn_id='redshift_default',
        task_id="update_intercom_deleted_conversations",
        sql="./sql/intercom_deleted_conversations.sql"
    )

    prev_task = None
    with TaskGroup('intercom_conversations', prefix_group_id=False) as intercom_conversations:
        group = config['intercom_conversations']
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

    intercom_admin = PostgresOperator(
        dag=dag,
        postgres_conn_id='redshift_default',
        task_id="update_intercom_admin_refresh",
        sql="./sql/intercom_admin.sql"
    )

    intercom_teams = PostgresOperator(
        dag=dag,
        postgres_conn_id='redshift_default',
        task_id="update_intercom_teams_refresh",
        sql="./sql/teams_for_assignment_intercom.sql"
    )

    short_circuit = ShortCircuitOperator(
        task_id="check_for_weekdays",
        ignore_downstream_trigger_rules=False,
        python_callable=lambda: dt.datetime.today().weekday() < 5
    )

    intercom_snapshot_cs_tables = RunQueryFromRepoOperator(
        dag=dag,
        task_id='intercom_snapshot_cs_tables',
        conn_id='redshift_default',
        directory=group['directory'],
        file="snapshot_tables",
    )

    intercom_deleted_conversations >> intercom_teams >> intercom_conversations \
        >> intercom_admin >> short_circuit >> intercom_snapshot_cs_tables
