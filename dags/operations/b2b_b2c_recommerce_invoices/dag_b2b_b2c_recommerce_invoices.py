import datetime
import logging

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'b2b_b2c_recommerce_invoices'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 2,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('0 10 * * 1-5'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'operations', 'b2b', 'b2c', 'returns', 'warehouse'])


process_recommerce_data = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="process_recommerce_data",
    sql="./sql/process_recommerce_data.sql"
)
