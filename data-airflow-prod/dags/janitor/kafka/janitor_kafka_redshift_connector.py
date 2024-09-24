import datetime
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from business_logic.janitor.kafka import redshift_connector
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.utils.kafka.connectors import get_connector_status

DAG_ID = 'janitor_kafka_redshift_connector'
CONFIG_VARIABLE = 'janitor_kafka_redshift_connector'

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 11, 6),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=5)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Restart Redshift connector in case of degradation',
    schedule_interval=get_schedule('0 * * * *'),
    catchup=False,
    max_active_runs=1,
    tags=['janitor', 'kafka', 'redshift']
)


get_connector_status = PythonOperator(
    dag=dag,
    task_id='get_connector_status',
    python_callable=get_connector_status,
    params={'config_variable': CONFIG_VARIABLE}
)

restart_connector = PythonOperator(
    dag=dag,
    task_id='restart_connector',
    python_callable=redshift_connector.restart_connector,
    params={'xcom_task_id': get_connector_status.task_id,
            'config_variable': CONFIG_VARIABLE}
)

skip_execution = EmptyOperator(
    dag=dag,
    task_id='skip_execution'
)

branching = BranchPythonOperator(
    dag=dag,
    task_id='branching',
    python_callable=redshift_connector.continue_execution,
    params={'xcom_task_id': get_connector_status.task_id,
            'continue_task_id': restart_connector.task_id,
            'skip_task_id': skip_execution.task_id
            }
    )


get_connector_status >> branching >> [skip_execution, restart_connector]
