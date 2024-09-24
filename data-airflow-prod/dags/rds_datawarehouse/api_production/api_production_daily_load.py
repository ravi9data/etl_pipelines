import datetime
import logging

from airflow import DAG

from dags.rds_datawarehouse.api_production.config.daily_load import config
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.s3.db_to_s3_operator import DbToS3Operator

DAG_ID = 'api_production_daily_load'
SOURCE_CONN_ID = 'rds_datawarehouse_read_replica'

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 12, 16),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=15),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=60)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='DAG for api_production incremental load, daily 01:00, from RDS dwh to datalake',
    schedule_interval=get_schedule('0 1 * * *'),
    catchup=False,
    max_active_runs=1,
    tags=[DAG_ID, 'api_production', 'daily', 'rds', 'postgres', 'datawarehouse']
)

for conf in config:
    data_load = DbToS3Operator(
        dag=dag,
        task_id=f'load_{conf.get("source_table")}',
        executor_config=conf.get('executor_config'),
        is_full_load=conf.get("is_full_load"),
        type_of_source_database='postgres',
        source_conn_id=SOURCE_CONN_ID,
        source_uid_column=conf.get('source_uid_column'),
        source_timestamp_field=conf.get('source_timestamp_field'),
        source_schema=conf.get('source_schema'),
        source_table=conf.get('source_table'),
        cta_glue_database='{{ var.value.glue_db_temp }}',
        target_s3_bucket='{{ var.value.data_raw_target_s3_bucket }}',
        target_s3_prefix=conf.get('target_s3_prefix'),
        target_glue_database='{{ var.value.api_production_target_glue_database }}',
        target_workgroup='{{ var.value.athena_workgroup }}',
        target_glue_table=conf.get('target_glue_table'),
        partition_columns=['year', 'month', 'day', 'hour'],
        file_format='parquet',
        file_compression_format='snappy',
        chunk_size=10000,
        max_rows_by_file=10000,
        all_columns_as_string=True,
        json_encoding=conf.get('json_encoding', False),
    )
