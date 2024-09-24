import datetime
import logging

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

DAG_ID = 'gdpr_services'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 2,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('0 22 * * *'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'gdpr', 'internal'])

stage_gdpr_service_data = AirbyteTriggerSyncOperator(
    task_id='airbyte_stage_gdpr_service_data',
    airbyte_conn_id='airbyte_prod',
    connection_id='02619535-ca27-4179-a9ba-da38bf87cd32',
    asynchronous=False,
    timeout=3600,
    wait_seconds=60,
    dag=dag
)

customer_info_external_services = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="customer_info_external_services",
    sql="./sql/customer_info_external_services.sql"
)

EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '3G',
        'request_cpu': '500m',
        'limit_memory': '20G',
        'limit_cpu': '3500m'
    }
}


def get_alchemy_connection(redshift_conn_id: str):
    logger.info('Establishing connection to Redshift')
    src_postgres_con = RedshiftSQLHook(aws_conn_id=redshift_conn_id).get_sqlalchemy_engine()
    logger.info('Connection is successful')
    return src_postgres_con


def check_customer_id(redshift_conn_id: str):
    engine = get_alchemy_connection(redshift_conn_id)
    query_1 = """
    SELECT customer_id
    FROM staging_airbyte_bi.gdpr_input
    """
    query_2 = """
        SELECT customer_id
        FROM hightouch_sources.gdpr_customer_info
        """
    logger.info('Executing:')
    logger.info(query_1)
    df_1 = pd.read_sql_query(
        sql=query_1,
        con=engine)

    df_2 = pd.read_sql_query(
        sql=query_2,
        con=engine)
    df_1 = df_1.astype({"customer_id": str})
    df_2 = df_2.astype({"customer_id": str})

    new_customer = set([i for i in df_1['customer_id']])
    existing_customer = set([i for i in df_2['customer_id']])

    if existing_customer == new_customer:
        logger.info('customer_id is same as existing customer')
        return 'end'
    else:
        return 'gdpr_customer_info'


check_customer = BranchPythonOperator(
        dag=dag,
        task_id='check_customer',
        python_callable=check_customer_id,
        op_kwargs={
            'redshift_conn_id': 'redshift_default'
            },
        executor_config=EXECUTOR_CONFIG
    )

end = EmptyOperator(
        dag=dag,
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED
    )

gdpr_customer_info = RunQueryFromRepoOperator(
    dag=dag,
    task_id='gdpr_customer_info',
    conn_id='redshift_default',
    directory='8_monitoring/gdpr',
    file='gdpr_customer_info',
)

gdpr_order_info = RunQueryFromRepoOperator(
    dag=dag,
    task_id='gdpr_order_info',
    conn_id='redshift_default',
    directory='8_monitoring/gdpr',
    file='gdpr_order_info',
)

gdpr_payment_info = RunQueryFromRepoOperator(
    dag=dag,
    task_id='gdpr_payment_info',
    conn_id='redshift_default',
    directory='8_monitoring/gdpr',
    file='gdpr_payment_info',
)

gdpr_login_traffic = RunQueryFromRepoOperator(
    dag=dag,
    task_id='gdpr_login_traffic',
    conn_id='redshift_default',
    directory='8_monitoring/gdpr',
    file='gdpr_login_traffic',
)

gdpr_consent = RunQueryFromRepoOperator(
    dag=dag,
    task_id='gdpr_consent',
    conn_id='redshift_default',
    directory='8_monitoring/gdpr',
    file='gdpr_consent',
)

gdpr_credit_bureau = RunQueryFromRepoOperator(
    dag=dag,
    task_id='gdpr_credit_bureau',
    conn_id='redshift_default',
    directory='8_monitoring/gdpr',
    file='gdpr_credit_bureau',
)

gdpr_onfido = RunQueryFromRepoOperator(
    dag=dag,
    task_id='gdpr_onfido',
    conn_id='redshift_default',
    directory='8_monitoring/gdpr',
    file='gdpr_onfido',
)

gdpr_communication = RunQueryFromRepoOperator(
    dag=dag,
    task_id='gdpr_communication',
    conn_id='redshift_default',
    directory='8_monitoring/gdpr',
    file='gdpr_communication',
)

gdpr_personal_info = RunQueryFromRepoOperator(
    dag=dag,
    task_id='gdpr_personal_info',
    conn_id='redshift_default',
    directory='8_monitoring/gdpr',
    file='gdpr_personal_info',
)

stage_gdpr_service_data >> customer_info_external_services >> check_customer >> end
(stage_gdpr_service_data >> customer_info_external_services >> check_customer >>
 gdpr_customer_info >> gdpr_order_info >> gdpr_payment_info >>
 gdpr_login_traffic >> gdpr_consent >> gdpr_credit_bureau >> gdpr_onfido >>
 gdpr_communication >> gdpr_personal_info >> end)
