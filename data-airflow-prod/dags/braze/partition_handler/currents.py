import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.braze.partitions_handler import get_partitions_values
from dags.braze.partition_handler.config import TABLES
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.datalake.glue_partitions_handler_operator import \
    GluePartitionHandlerOperator

logger = logging.getLogger(__name__)

DAG_ID = 'braze_partition_handler'

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 7, 8),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=5),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=120)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Add new partitions to braze tables',
    schedule_interval=get_schedule('30 * * * *'),
    catchup=False,
    max_active_runs=1,
    tags=[DAG_ID, 'braze', 'partitions']
)

get_partitions_values_task = PythonOperator(
    dag=dag,
    task_id='get_partitions_values',
    python_callable=get_partitions_values,
    op_kwargs={
        'config_variable': DAG_ID
    }
)

for table in TABLES:

    add_partitions = GluePartitionHandlerOperator(
        dag=dag,
        task_id=f'add_partitions_to_{table}',
        glue_database='{{ var.value.glue_db_braze_currents }}',
        glue_table=table,
        partitions="{{ ti.xcom_pull(task_ids='get_partitions_values') }}"
    )

    get_partitions_values_task >> add_partitions
