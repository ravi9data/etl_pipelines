import datetime
import logging

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'recommerce_masterliste_mapping_step_1'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 3,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('45 8 * * 1-5'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'operations', 'recommerce', 'masterliste_mapping'])

distinct_foxway_masterliste = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="distinct_foxway_masterliste",
    sql="./sql/distinct_foxway_masterliste.sql"
)

foxway_masterliste_serial_mismatch = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="foxway_masterliste_serial_mismatch",
    sql="./sql/foxway_masterliste_serial_mismatch.sql"
)

foxway_masterliste_serial_mismatch_high_touch = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="foxway_masterliste_serial_mismatch_high_touch",
    sql="./sql/foxway_masterliste_serial_mismatch_high_touch.sql"
)

foxway_masterliste_mapping = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="foxway_masterliste_mapping",
    sql="./sql/foxway_masterliste_mapping.sql"
)

distinct_foxway_masterliste >> foxway_masterliste_serial_mismatch >> \
    foxway_masterliste_serial_mismatch_high_touch >> foxway_masterliste_mapping
