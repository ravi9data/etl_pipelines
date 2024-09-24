import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.datalake.glue_partitions_handler_operator import \
    GluePartitionHandlerOperator
from plugins.s3_utils import get_partitions_values

DAG_ID = 'segment_partitions_handler'


logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 6, 13),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=15),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=5)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Add new partitions to Segment table ingested via Firehose',
    # setup the schedule accordingly based on the firehose delivery buffer time
    schedule_interval=get_schedule('30 * * * *'),
    catchup=False,
    max_active_runs=1,
    tags=[DAG_ID, 'segment', 'partitions']
)


get_partitions_values = PythonOperator(
    dag=dag,
    task_id='get_partitions_values',
    python_callable=get_partitions_values,
    op_kwargs={
        'config_variable': DAG_ID
    }
)

add_partitions_grover_js_raw = GluePartitionHandlerOperator(
    dag=dag,
    task_id='add_partitions',
    glue_database='{{ var.value.glue_db_segment }}',
    glue_table='grover_js_raw',
    partitions="{{ ti.xcom_pull(task_ids='get_partitions_values') }}"
)

get_partitions_values >> add_partitions_grover_js_raw
