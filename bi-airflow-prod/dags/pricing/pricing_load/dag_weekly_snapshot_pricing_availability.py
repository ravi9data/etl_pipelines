import datetime
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'weekly_snapshot_pricing_availability_ideal_state'
REDSHIFT_CONN = 'redshift_default'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 11, 4),
                'retries': 2,
                'retry_delay': timedelta(seconds=15),
                'retry_exponential_backoff': True,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID, default_args=default_args,
          description='DAG to load Weekly snapshot pricing_availability_ideal_state',
          schedule_interval=get_schedule('0 17 * * 5'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'pricing', 'weekly', 'snapshot'])

t0 = EmptyOperator(task_id="begin", dag=dag)
t1 = EmptyOperator(task_id="end", dag=dag)

run_availability_ideal_state_snapshot_sql = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="availability_ideal_state_snapshot",
    sql="./sql/availability_ideal_state_snapshot.sql"
)

t0 >> run_availability_ideal_state_snapshot_sql >> t1
