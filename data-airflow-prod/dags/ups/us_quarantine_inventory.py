import datetime
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from business_logic.ups.us_quarantine_inventory import \
    extract_email_attachment_to_s3
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.utils.recommerce import slack_message_dataload_notifications

DAG_ID = 'ups_us_quarantine_inventory'
REDSHIFT_CONN = 'redshift'
EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'limit_memory': '1G',
        'limit_cpu': '1500m'
    }
}

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 6, 11),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=120),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=15)
}

doc_md = """
## Retrieves the email attachment from `us-operations-group@grover.com` sent to `dataengsys@grover.com`.

- The email subject is "Quarantine Inventory On Hand". There is no fixed schedule when the email arrives.
- This DAG is scheduled daily. If the email is not available, a Slack notification is sent to #dataload-notifications.
- Loads the written data to Redshift table `stg_external_apis.ups_us_quarantine_inventory`.
- Also writes the data as CSV format to `s3://grover-eu-central-1-production-data-raw/ups/us_quarantine_inventory/`.
- And in Glue: `data_production_ups.us_quarantine_inventory`.
### Notes
- Variable `email_date` can be optionally supplied as "YYYY-MM-DD" string, to enable backfilling
of a specific date when the email is received.
- The `email_date` is also the `reporting_date` in the table.
"""  # noqa: E501


dag = DAG(
    DAG_ID,
    doc_md=doc_md,
    default_args=default_args,
    description='Extract data from UPS Quarantine Inventory email attachment and loads to Redshift',
    schedule_interval=get_schedule('0 10 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=['ups', 'us', 'inventory', DAG_ID]
)

extract_email_attachment_to_s3 = BranchPythonOperator(
        dag=dag,
        task_id='extract_email_attachment_to_s3',
        python_callable=extract_email_attachment_to_s3,
        op_kwargs={
            'dag_config': f'{DAG_ID}_settings'
            },
        executor_config=EXECUTOR_CONFIG
    )

redshift_group = TaskGroup(
        dag=dag,
        group_id='redshift'
    )

create_target_table = PostgresOperator(
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN,
        task_id="create_target_table",
        sql="./sql/create_table.sql",
        task_group=redshift_group
    )

stage_data = PostgresOperator(
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN,
        task_id="stage_data",
        sql="./sql/stage_data.sql",
        task_group=redshift_group
    )

upsert_data = PostgresOperator(
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN,
        task_id="upsert_data",
        sql="./sql/upsert_data.sql",
        autocommit=False,
        task_group=redshift_group
    )

clean_up = PostgresOperator(
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN,
        task_id="clean_up",
        sql="./sql/clean_up.sql",
        autocommit=True,
        task_group=redshift_group
    )

slack_message_success = PythonOperator(
        dag=dag,
        task_id='slack_message_success',
        python_callable=slack_message_dataload_notifications,
        op_kwargs={
            'dag_config': f'{DAG_ID}_settings',
            'msg_topic': 'UPS Quarantine Inventory at Hand',
            'xcom_date_key': 'reporting_date'
            }
    )

slack_message_failure = PythonOperator(
        dag=dag,
        task_id='slack_message_failure',
        python_callable=slack_message_dataload_notifications,
        op_kwargs={
            'dag_config': f'{DAG_ID}_settings',
            'failure': True,
            }
    )

end = EmptyOperator(
        dag=dag,
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED
    )

extract_email_attachment_to_s3 >> slack_message_failure >> end
(extract_email_attachment_to_s3 >> stage_data >> create_target_table
    >> upsert_data >> clean_up >> slack_message_success >> end)
