import datetime
import logging

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

from business_logic.intercom.commons import (get_latest_source_files,
                                             move_files_to_dest,
                                             slack_message_no_delta)
from business_logic.intercom.conversations_export import (
    denest_conversations_parts, transform_raw_conversations)
from plugins.dag_utils import (generate_batch_id, get_schedule,
                               on_failure_callback)

DAG_ID = 'intercom_conversations_export'
DAG_CONFIG = f'{DAG_ID}_config'
REDSHIFT_CONN = 'redshift'
EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '3G',
        'request_cpu': '1000m',
        'limit_memory': '6G',
        'limit_cpu': '2000m'
    }
}

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 2, 15),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=180),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=60)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Read exported files from Intercom and store in S3 & Redshift',
    schedule_interval=get_schedule('0 5 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=['intercom', 'conversations', 'export', 'redshift', DAG_ID]
)

start = EmptyOperator(
        dag=dag,
        task_id='start'
    )

generate_batch_id = PythonOperator(
        dag=dag,
        task_id='generate_batch_id',
        python_callable=generate_batch_id
    )

get_latest_source_files = BranchPythonOperator(
        dag=dag,
        task_id='get_latest_source_files',
        python_callable=get_latest_source_files,
        op_kwargs={'dag_config': DAG_CONFIG},
        executor_config=EXECUTOR_CONFIG
    )

copy_source_files = PythonOperator(
        dag=dag,
        task_id='copy_source_files',
        python_callable=move_files_to_dest,
        op_kwargs={
            'delta_files': '{{ var.json.intercom_conversations_export_config.s3_csv_path }}',
            's3_src_path': '{{ var.json.intercom_conversations_export_config.s3_src_path }}',
            's3_dest_path': '{{ var.json.intercom_conversations_export_config.s3_dest_path }}'
            }
    )

transform_raw_conversations = PythonOperator(
        dag=dag,
        task_id='transform_raw_conversations',
        python_callable=transform_raw_conversations,
        op_kwargs={'dag_config': DAG_CONFIG},
        executor_config=EXECUTOR_CONFIG
    )

denest_conversations_parts = PythonOperator(
        dag=dag,
        task_id='denest_conversations_parts',
        python_callable=denest_conversations_parts,
        executor_config=EXECUTOR_CONFIG)

staging_conversations = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="staging_conversations",
    sql="./sql/conversations_export/stage_conversations.sql",
    execution_timeout=datetime.timedelta(minutes=15)
)

staging_conversations_parts = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="staging_conversation_parts",
    sql="./sql/conversations_export/stage_conversations_parts.sql",
    execution_timeout=datetime.timedelta(minutes=15)
)

upsert_conversations = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="upsert_conversations",
    sql="./sql/conversations_export/upsert_conversations.sql",
    autocommit=False,
    execution_timeout=datetime.timedelta(minutes=10)
)

upsert_conversations_parts = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="upsert_conversation_parts",
    sql="./sql/conversations_export/upsert_conversations_parts.sql",
    autocommit=False,
    execution_timeout=datetime.timedelta(minutes=10)
)

clean_up = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="clean_up",
    sql="./sql/conversations_export/clean_up.sql",
    autocommit=False,
    execution_timeout=datetime.timedelta(minutes=5)
)


slack_message_no_delta = PythonOperator(
        dag=dag,
        task_id='slack_message_no_delta',
        python_callable=slack_message_no_delta,
        op_kwargs={'dag_config': DAG_CONFIG}
    )

end = EmptyOperator(
        dag=dag,
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED
    )


start >> generate_batch_id >> get_latest_source_files >> slack_message_no_delta >> end
chain(start, generate_batch_id, get_latest_source_files, copy_source_files,
      transform_raw_conversations, denest_conversations_parts,
      [staging_conversations, staging_conversations_parts],
      [upsert_conversations, upsert_conversations_parts],
      clean_up, end)
