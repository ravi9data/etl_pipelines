import datetime
import os

from airflow import DAG

from plugins.dag_utils import on_failure_callback
from plugins.operators.SodaChecksOperator import SodaChecksOperator

DAG_ID = 'data_quality_checks_daily'

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 11, 6),
    'retries': 0,
    'retry_delay': datetime.timedelta(seconds=15),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=15)
}

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

dag = DAG(
    DAG_ID,
    default_args=default_args,
    start_date=datetime.datetime(2021, 11, 6),
    description='Daily data quality checks',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID, 'soda', 'quality_checks']
)

directory = os.path.join(__location__, '../../business_logic/data_quality/checks/master')

for filename in os.listdir(directory):
    check_file_location = os.path.join(directory, filename)
    master_data_checks = SodaChecksOperator(
        task_id=f"master_{filename.split(sep='.')[0]}_data_checks",
        dag=dag,
        soda_checks_path=check_file_location,
        schema_name='master',
        log_results=True
    )
