import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from dags.kinesis_events.config.config import tables_for_partitioning
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.datalake.glue_partitions_handler_operator import \
    GluePartitionHandlerOperator
from plugins.s3_utils import get_partitions_values

DAG_ID = 'kinesis_events_partition_handler'
DOC_MD = """
#### Adds partitions to the Glue tables after checking if files are present in the
#### related S3 bucket path.

#### List of kinesis events tables:

- `grover_product_impression`
- `grover_address_view`
- `grover_button_impression`
- `grover_cart_view`
- `grover_failed_login_view`
- `grover_login_view`

Location: s3://grover-eu-central-1-production-data-raw/kinesis_events/

The DAG schedule is based on the estimated Firehose delivery buffer time.
"""  # noqa: E501


logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 7, 4),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=5),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=10)
}

dag = DAG(
    DAG_ID,
    doc_md=DOC_MD,
    default_args=default_args,
    description='Add new partitions to Kinesis events Glue tables ingested via Firehose',
    schedule_interval=get_schedule('15,30,45 * * * *'),
    catchup=False,
    max_active_runs=1,
    tags=[DAG_ID, 'kinesis_events', 'partitions']
)

get_partitions_values = PythonOperator(
        dag=dag,
        task_id='get_partitions_values',
        python_callable=get_partitions_values,
        op_kwargs={
            'config_variable': f'{DAG_ID}_settings'
            }
    )

for glue_table in tables_for_partitioning:
    add_partitions = GluePartitionHandlerOperator(
        dag=dag,
        task_id=f'add_partitions_{glue_table}',
        glue_database='{{ var.value.glue_db_kinesis_events_raw }}',
        glue_table=glue_table,
        partitions="{{ ti.xcom_pull(task_ids='get_partitions_values') }}"
        )

    get_partitions_values >> add_partitions
