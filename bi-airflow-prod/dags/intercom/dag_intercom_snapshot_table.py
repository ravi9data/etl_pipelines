import datetime as dt
import logging

from airflow import DAG

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

logger = logging.getLogger(__name__)

DAG_ID = 'intercom_snapshot_table_refresh'

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': dt.datetime(2022, 12, 13),
    'retries': 2,
    'on_failure_callback': on_failure_callback
}
dag = DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval=get_schedule('0 4 * * 1-5'),
        max_active_runs=1,
        dagrun_timeout=dt.timedelta(minutes=180),
        tags=[DAG_ID, 'intercom', 'cs'])


intercom_snapshot_cs_tables = RunQueryFromRepoOperator(
        dag=dag,
        conn_id='redshift_default',
        task_id='intercom_snapshot_cs_tables',
        directory='91_External/Intercom',
        file='snapshot_tables'
    )
