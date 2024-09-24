import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

from dags.staging.salesforce.config import sf_hist_obj_sync_config
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.salesforce_schema_to_redshift_operator import \
    SalesforceSchemaToRedshiftOperator
from plugins.operators.salesforce_to_s3_operator import SalesforceToS3Operator
from plugins.utils.salesforce import get_soql_inc_query

SF_CONN_ID = 'sf_prod'
S3_CONN_ID = 'aws_default'
REDSHIFT_CONN_ID = 'redshift_default'

DAG_ID = 'salesforce_history_to_redshift_refresh'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 2,
                'on_failure_callback': on_failure_callback}

EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '5G',
        'request_cpu': '500m',
        'limit_memory': '20G',
        'limit_cpu': '6000m'
    }
}

with DAG(DAG_ID,
         default_args=default_args,
         description='DAG to sync object history data from Salesforce',
         schedule_interval=get_schedule('@daily'),
         max_active_runs=1,
         catchup=False,
         tags=[DAG_ID, 'salesforce'],
         ) as dag:
    s3_bucket = '{{ var.value.s3_bucket_curated_bi }}'
    config = sf_hist_obj_sync_config

    upsert = PostgresOperator(
        task_id='run_upsert_sql',
        sql='./sf_history_stg_upsert.sql',
        postgres_conn_id=REDSHIFT_CONN_ID,
        autocommit=True,
        dag=dag
    )

    for rec in config:
        sf_object = str(rec['name']).lower()
        load_type = rec.get('load_type', 'incremental')

        with TaskGroup(f'{sf_object}_refresh', prefix_group_id=False) as sf_obj_refresh:
            s3_key = 'salesforce_history/{}/{}.json'.format('{{ execution_date }}', sf_object)

            get_soql_query = PythonOperator(
                task_id=f"get_{sf_object}_soql_query",
                python_callable=get_soql_inc_query,
                op_kwargs={"object_name": sf_object,
                           "sf_conn_id": SF_CONN_ID,
                           "rs_conn_id": REDSHIFT_CONN_ID,
                           "load_type": load_type,
                           "target_schema": 'stg_salesforce'
                           },
                dag=dag
            )

            salesforce_to_s3 = SalesforceToS3Operator(
                task_id=f'{sf_object}_to_S3',
                sf_conn_id=SF_CONN_ID,
                sf_obj=sf_object,
                fmt='ndjson',
                query=f"{{{{ ti.xcom_pull(task_ids='get_{sf_object}_soql_query') }}}}",
                load_type=load_type,
                s3_conn_id=S3_CONN_ID,
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                coerce_to_timestamp=True,
                executor_config=EXECUTOR_CONFIG,
                dag=dag)

            s3_to_redshift = SalesforceSchemaToRedshiftOperator(
                task_id=f'{sf_object}_to_Redshift',
                sf_conn_id=SF_CONN_ID,
                s3_conn_id=S3_CONN_ID,
                rs_conn_id=REDSHIFT_CONN_ID,
                sf_object=sf_object,
                rs_schema='stg_salesforce_dl',
                rs_table=sf_object,
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                db_creds='aws_iam_role={{var.value.redshift_iam_role}}',
                dag=dag)

            get_soql_query >> salesforce_to_s3 >> s3_to_redshift

        sf_obj_refresh >> upsert
