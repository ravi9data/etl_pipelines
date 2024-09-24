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
    'start_date': datetime.datetime(2023, 4, 5),
    'retries': 2,
    'on_failure_callback': on_failure_callback}

DAG_ID = 'risk_monitoring'

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=get_schedule('10 6,7 * * *'),
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['risk', 'redshift' 'monitoring'])

begin = EmptyOperator(dag=dag, task_id="begin")
end = EmptyOperator(dag=dag, task_id="end")

run_risk_monitoring_sql = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="run_risk_monitoring_sql",
    sql="./sql/risk_monitoring.sql"
)

begin >> run_risk_monitoring_sql >> end
