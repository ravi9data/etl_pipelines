import datetime
import logging

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'foxway_masterliste'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 2,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('0 11 * * 1-5'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'operations', 'foxway'])


process_foxway_data = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="process_foxway_data",
    sql="./sql/process_foxway_masterliste_data.sql"
)
