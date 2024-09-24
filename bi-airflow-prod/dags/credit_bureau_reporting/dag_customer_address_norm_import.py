import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.credit_bureau_reporting.import_customer_address_norm_data import \
    import_normalized_addresses
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'customer_address_norm_import'

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bi',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 10, 12),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=5)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Process Customer address normalisation file every Tuesday',
    schedule_interval=get_schedule('0 8 * * TUE'),
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=15),
    tags=['Automation', 'Risk']
)

import_normalized_addresses_file = PythonOperator(
    dag=dag,
    task_id='import_normalized_addresses_file',
    python_callable=import_normalized_addresses
)
