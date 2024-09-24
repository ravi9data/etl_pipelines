import datetime
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from dags.fx_rates.exhange_rate_api import extract_exchange_rates
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'load_usd_exchanges_rates'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng', 'depends_on_past': False,
                'start_date': datetime.datetime(2022, 8, 8), 'retries': 2,
                'retry_delay': datetime.timedelta(seconds=15), 'retry_exponential_backoff': True,
                'on_failure_callback': on_failure_callback}

with DAG(DAG_ID, default_args=default_args,
         description='DAG to fetch exchange rate from API',
         schedule_interval=get_schedule('0 6-19/12 * * *'), max_active_runs=1, catchup=False,
         tags=[DAG_ID, 'exchange_rates', 'redshift']) as dag:
    t0 = EmptyOperator(task_id='start')

    load_exchange_rate = PythonOperator(
        dag=dag,
        task_id='load_exchange_rate',
        python_callable=extract_exchange_rates
    )

    t0 >> load_exchange_rate
