import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from business_logic.ingram.process_ogs import ingram_ogs_sftp_to_s3
from plugins.dag_utils import on_failure_callback

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': datetime(2022, 8, 11),
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
    'on_failure_callback': on_failure_callback}

DAG_ID = 'iogs_historical_load'

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=['redshift'])


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


# config = {{'var.json.json_variable.iogs_historical_load_config'}}

start_date = datetime.strptime(Variable.get('iogs_start', default_var='2023-04-01'), '%Y-%m-%d')
end_date = datetime.strptime(Variable.get('iogs_end', default_var='2023-04-05'), '%Y-%m-%d')

for run_date in daterange(start_date, end_date):
    run_upsert_from_rds = PythonOperator(
        dag=dag,
        task_id=f"load_sftp_data_{run_date.strftime('%Y%m%d')}",
        python_callable=ingram_ogs_sftp_to_s3,
        op_kwargs={'run_date': run_date.strftime('%Y%m%d')}
    )
