import datetime
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 8, 31),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=5)}

DAG_ID = '4r_recommerce_stages'

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=get_schedule('30 10 * * *'),
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10),
    tags=['recommerce', 'redshift', '4R', 'recommerce_stages'])

begin = EmptyOperator(dag=dag, task_id="begin")
end = EmptyOperator(dag=dag, task_id="end")

run_recommerce_stages_sql = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="run_recommerce_stages_sql",
    sql="./sql/recommerce_stages.sql"
)

run_recommerce_stages_financial_sql = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="run_recommerce_stages_financial_sql",
    sql="./sql/recommerce_stages_financial.sql"
)

begin >> run_recommerce_stages_sql >> run_recommerce_stages_financial_sql >> end
