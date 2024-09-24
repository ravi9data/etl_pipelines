import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.update_sf_b2b.update_sf import update_account, update_lead
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'update_sf_b2b_custom_fields'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 2,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          description='DAG to fetch exchange rate from API',
          schedule_interval=get_schedule('0 8 * * *'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'salesforce', 'b2b'])

update_b2b_account = PythonOperator(
    dag=dag,
    task_id='update_b2b_account',
    python_callable=update_account
)

update_b2b_lead = PythonOperator(
    dag=dag,
    task_id='update_b2b_lead',
    python_callable=update_lead
)
