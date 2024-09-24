import datetime
import logging
import os

from airflow import DAG

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.datalake_to_redshfit_operator import \
    DatalakeToRedshiftOperator

DAG_ID = 'risk_eu_order_decision_final_v1'

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 12, 1),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=15)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='curation layer for risk_eu_order_decision_final_v1',
    schedule_interval=get_schedule('*/30 * * * *'),
    max_active_runs=1,
    catchup=False,
    params={'load_type': 'incremental', 'inc_load_hrs': '2'},
    tags=['cdc', DAG_ID, 'curation']
)

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

glue_table_name = 'risk_eu_order_decision_final_v1'
sql_file_path = os.path.join(__location__, f'datalake_sql/{glue_table_name}.sql')

inc_load_last_n_hrs = DatalakeToRedshiftOperator(
    dag=dag,
    task_id='load_from_datalake_to_redshift',
    load_type='{{ params.load_type }}',
    glue_database='data_production_kafka_topics_raw',
    sql_file_path=sql_file_path,
    s3_prefix=glue_table_name,
    redshift_conn_id='redshift_default',
    target_table=glue_table_name,
    target_schema='stg_curated',
    inc_load_hrs='{{ params.inc_load_hrs }}',
)

inc_load_last_n_hrs
