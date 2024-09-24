import datetime
import logging
import os

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_sql import \
    RedshiftSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import \
    S3ToRedshiftOperator

from business_logic.customer_labels4.config import (BUCKET_NAME, DAG_ID,
                                                    EXECUTOR_CONFIG,
                                                    PIPELINE_OUTPUT_PATH,
                                                    REDSHIFT_SCHEMA,
                                                    TABLE_COLUMNS,
                                                    TEMP_TABLE_NAME)
from business_logic.customer_labels4.customer_labels_task import \
    customer_labels_task
from business_logic.customer_labels4.query import (create_redshift_tables,
                                                   update_historical_table)

default_args = {
    "retries": 2,
    "retry_delay": datetime.timedelta(seconds=5),
}


dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime.datetime(2024, 7, 9),
    schedule_interval="45 23 * * *",
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID, "customer_labels4"],
)

t0 = DummyOperator(task_id="start", dag=dag)

create_labels_task = PythonOperator(
    dag=dag,
    task_id="customer_labels_task",
    python_callable=customer_labels_task,
    op_kwargs={
        "current_time": "{{ data_interval_end.strftime('%Y-%m-%d') }}"
    },
    executor_config=EXECUTOR_CONFIG
)

create_tmp_table_task = create_table_task = RedshiftSQLOperator(
    task_id="create_redshift_tables",
    sql=create_redshift_tables(),
    redshift_conn_id="redshift",
    dag=dag,
    autocommit=False
)

load_to_tmp_table_task = S3ToRedshiftOperator(
    dag=dag,
    task_id="load_to_tmp_table",
    schema=REDSHIFT_SCHEMA,
    table=TEMP_TABLE_NAME,
    s3_bucket=BUCKET_NAME,
    s3_key=PIPELINE_OUTPUT_PATH.replace(
        "date_re",
        "{{ data_interval_end.strftime('%Y-%m-%d') }}"
    ),
    copy_options=[
        "CSV",
        "IGNOREHEADER 1",
        "DATEFORMAT 'auto'",
        "TIMEFORMAT 'auto'",
        "COMPUPDATE OFF",
        "STATUPDATE OFF",
    ],
    column_list=TABLE_COLUMNS,
    method="APPEND",
    redshift_conn_id="redshift",
    aws_conn_id="aws_default",
    autocommit=False
)

update_historical_table_task = RedshiftSQLOperator(
    task_id="update_historical_table",
    sql=update_historical_table("{{ data_interval_end.strftime('%Y-%m-%d') }}"),
    redshift_conn_id="redshift",
    dag=dag,
    autocommit=False
)

t0 >> create_labels_task >> create_tmp_table_task >> load_to_tmp_table_task >> update_historical_table_task
