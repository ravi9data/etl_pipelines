import datetime
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

from business_logic.recommerce.ingram_micro.send_order_grading_status import (
    get_source_files_delta, transform_raw_ingram_source_files)
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.s3.sftp_to_s3_mutiplefiles_operator import \
    SFTPToS3MultipleFilesOperator
from plugins.utils.recommerce import (slack_message_dataload_notifications,
                                      slack_message_no_delta)

DAG_ID = 'ingram_micro_send_order_grading_status_v1'
DAG_CONFIG = f'{DAG_ID}_config'
REDSHIFT_CONN = 'redshift_default'
INGRAM_MICRO_CONN = 'ingram_micro_sftp_conn'
EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '500Mi',
        'request_cpu': '200m',
        'limit_memory': '20G',
        'limit_cpu': '3500m'
    }
}

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 11, 9),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=120),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    # 'execution_timeout': datetime.timedelta(minutes=30)
}

DOC_MD = """
###
1-compares the json files in SFTP and recommerce.ingram_micro_send_order_grading_status
2-Downloads the json files which don't exist in recommerce.ingram_micro_send_order_grading_status from remote SFTP server `ftp.ims-flensburg.com` and stores in S3
3-Reads the json files from S3 and load into staging.ingram_sftp_daily_events
4-Inserts the data in staging.ingram_sftp_daily_events into recommerce.ingram_micro_send_order_grading_status
"""   # noqa: E501

dag = DAG(
    DAG_ID,
    doc_md=DOC_MD,
    default_args=default_args,
    description='Extract data from IngramMicro SFTP server and load to Redshift.',
    schedule_interval=get_schedule('0 05 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=['recommerce', 'IngramMicro', 'sftp', DAG_ID]
)

get_source_files_for_download = BranchPythonOperator(
        dag=dag,
        task_id='get_source_files_for_download',
        python_callable=get_source_files_delta,
        op_kwargs={
            'dag_config': DAG_CONFIG,
            'redshift_conn_id': REDSHIFT_CONN,
            'source_sftp_conn_id': INGRAM_MICRO_CONN
            },
        executor_config=EXECUTOR_CONFIG
    )

download_delta_source_files_to_s3 = SFTPToS3MultipleFilesOperator(
        dag=dag,
        task_id='download_delta_source_files_to_s3',
        destination_s3_bucket='{{ var.json.ingram_micro_send_order_grading_status_v1_config.destination_s3_bucket }}',   # noqa: E501
        destination_s3_prefix='{{ var.json.ingram_micro_send_order_grading_status_v1_config.destination_s3_prefix }}',   # noqa: E501
        source_files_to_download_csv_path='{{ var.json.ingram_micro_send_order_grading_status_v1_config.source_files_to_download_csv_path }}',   # noqa: E501
        source_sftp_conn_id=INGRAM_MICRO_CONN
        )


process_and_transform_raw_files = PythonOperator(
        dag=dag,
        task_id='process_and_transform_raw_files',
        python_callable=transform_raw_ingram_source_files,
        op_kwargs={
            'dag_config': DAG_CONFIG
            },
        executor_config=EXECUTOR_CONFIG
    )

insert_data = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id="insert_data",
    sql="./sql/grading_status/insert_ingram_micro_send_order_grading_status.sql"
)

slack_message_no_delta = PythonOperator(
        dag=dag,
        task_id='slack_message_no_delta',
        python_callable=slack_message_no_delta,
        op_kwargs={
            'dag_config': DAG_CONFIG,
            }
    )

slack_message_success = PythonOperator(
        dag=dag,
        task_id='slack_message_success',
        python_callable=slack_message_dataload_notifications,
        op_kwargs={
            'dag_config': DAG_CONFIG,
            'msg_topic': 'IngramMicro SFTP SendOrderGradingStatus'
            }
    )

end = EmptyOperator(
        dag=dag,
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED
    )

get_source_files_for_download >> slack_message_no_delta >> end
(get_source_files_for_download >> download_delta_source_files_to_s3 >>
 process_and_transform_raw_files >> insert_data >> slack_message_success >> end)
