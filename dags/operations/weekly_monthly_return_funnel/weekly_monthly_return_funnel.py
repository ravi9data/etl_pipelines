import datetime
import logging

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'weekly_monthly_return_finnel'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 3,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('0 9 * * 1-5'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'operations', 'weekly', 'monthly', 'funnel'])

weekly_return_finnel = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="weekly_return_finnel",
    sql="./sql/weekly_return_finnel.sql"
)

monthly_return_funnel = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="monthly_return_funnel",
    sql="./sql/monthly_return_funnel.sql"
)
