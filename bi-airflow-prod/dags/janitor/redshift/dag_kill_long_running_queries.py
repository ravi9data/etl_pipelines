import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from dags.janitor.redshift import kill_queries
from plugins.dag_utils import on_failure_callback

DAG_ID = 'kill_long_running_queries'
REDSHIFT_CONN = 'redshift_admin'

ENABLE_DELETE = True
PRINT_DELETES = True

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 3, 23),
    'retries': 2,
    'max_active_runs': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=5)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    tags=[DAG_ID, 'maintenance_dag']
)

update_user_groups_list = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="update_user_groups_list",
    sql="call bi_audit_schema.make_user_group();"
)

kill_long_running_queries = PythonOperator(
    dag=dag,
    task_id='kill_long_running_queries',
    python_callable=kill_queries.kill_long_running_queries,
)

update_user_groups_list >> kill_long_running_queries
