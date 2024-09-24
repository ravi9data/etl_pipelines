import datetime as dt
import logging

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

logger = logging.getLogger(__name__)

DAG_ID = 'manual_review_daily_pipeline'

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': dt.datetime(2022, 10, 4),
    'retries': 2,
    'on_failure_callback': on_failure_callback
}

with DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval=get_schedule('15 23 * * 1-5'),
        max_active_runs=1,
        catchup=False,
        dagrun_timeout=dt.timedelta(minutes=180),
        tags=[DAG_ID, 'manual_review', 'pipeline']) as dag:
    t0 = EmptyOperator(task_id="begin", dag=dag)
    t1 = EmptyOperator(task_id="end", dag=dag)

    stage_manual_review_data = AirbyteTriggerSyncOperator(
        task_id='airbyte_stage_manual_review_data_data',
        airbyte_conn_id='airbyte_prod',
        connection_id='6b59a7e9-5600-41a2-818e-1b99c82a6d16',
        asynchronous=False,
        timeout=3600,
        wait_seconds=60,
        dag=dag
    )

    load_manual_review_decisions = PostgresOperator(
        dag=dag,
        postgres_conn_id='redshift_default',
        task_id="load_manual_review_decisions",
        sql="./sql/manual_review_decisions.sql"
    )

    chain(
        t0,
        stage_manual_review_data,
        load_manual_review_decisions,
        t1
    )
