import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.credit_bureau_reporting.export_credit_reporting_data import \
    export_cb_report
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'credit_bureau_reporting_export'

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
    description='Send Credit Bureau reports every Thursday',
    schedule_interval=get_schedule('0 8 * * THU'),
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=15),
    tags=['Automation', 'Risk']
)

export_data = PythonOperator(
    dag=dag,
    task_id='send_credit_bureau_report',
    python_callable=export_cb_report
)
