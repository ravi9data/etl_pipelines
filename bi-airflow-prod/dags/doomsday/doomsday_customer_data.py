import datetime as dt
import logging

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'dooms_day'

logger = logging.getLogger(__name__)

default_args = {"owner": "bi-eng", "depends_on_past": False,
                "start_date": dt.datetime(2024, 7, 4),
                "retries": 2,
                "retry_delay": dt.timedelta(seconds=5),
                "retry_exponential_backoff": True,
                "on_failure_callback": on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          description="Extract Doomsday data from Redshift and load into s3",
          schedule_interval=get_schedule("0 11 * * *"),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, "DoomsDay", "Customer Data"])

doomsday_customer_data = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="doomsday_customer_data",
    sql="./sql/doomsday_customer_data.sql"
)
