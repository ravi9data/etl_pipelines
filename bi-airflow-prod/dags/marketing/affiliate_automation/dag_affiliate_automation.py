import datetime
import logging
import os

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import dags.marketing.affiliate_automation.fetch_daisycon_data as fdd
from dags.marketing.affiliate_automation.get_cj_conversions_from_sftp import \
    get_cj_conversions_from_sftp
from dags.marketing.affiliate_automation.get_everflow_conversions import \
    get_eflow_conversion
from dags.marketing.affiliate_automation.process_airflow_data import \
    cleanup_temp_affiliate_files_s3
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

DAG_ID = 'affiliate_automation'
REDSHIFT_CONN = 'redshift_default'

GOOGLE_CLOUD_CONN = 'google_cloud_default'

EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '500Mi',
        'request_cpu': '500m',
        'limit_memory': '2G',
        'limit_cpu': '1000m'
    }
}

logger = logging.getLogger(__name__)

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
end_date = datetime.datetime.now().strftime("%Y-%m-%d")

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 2,
                'retry_delay': datetime.timedelta(seconds=30),
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('15 5 * * *'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'affiliate', 'affiliate_automation'])

cleanup_temp_affiliate_files_s3 = PythonOperator(
    dag=dag,
    task_id="cleanup_temp_affiliate_files_s3",
    python_callable=cleanup_temp_affiliate_files_s3
)

fetch_everflow_data = PythonOperator(
    dag=dag,
    task_id="fetch_everflow_data",
    python_callable=get_eflow_conversion,
    op_kwargs={"start_date": start_date, "end_date": end_date, "page_size": 200}
)

fetch_cj_data = PythonOperator(
    dag=dag,
    task_id="fetch_cj_data",
    python_callable=get_cj_conversions_from_sftp
)

fetch_daisycon_data = PythonOperator(
    dag=dag,
    task_id='fetch_daisycon_data',
    python_callable=fdd.fetch_data
)

affiliate_everflow_staging = RunQueryFromRepoOperator(
    dag=dag,
    task_id='affiliate_everflow_staging',
    conn_id='redshift_default',
    directory='11_marketing_reporting/Affiliate_Automation',
    file='affiliate_everflow_staging',
)

affiliate_everflow_submitted_orders = RunQueryFromRepoOperator(
    dag=dag,
    task_id='affiliate_everflow_submitted_orders',
    conn_id='redshift_default',
    directory='11_marketing_reporting/Affiliate_Automation',
    file='affiliate_everflow_submitted_orders',
)

affiliate_daisycon_staging = RunQueryFromRepoOperator(
    dag=dag,
    task_id='affiliate_daisycon_staging',
    conn_id='redshift_default',
    directory='11_marketing_reporting/Affiliate_Automation',
    file='affiliate_daisycon_staging',
)

affiliate_daisycon_submitted_orders = RunQueryFromRepoOperator(
    dag=dag,
    task_id='affiliate_daisycon_submitted_orders',
    conn_id='redshift_default',
    directory='11_marketing_reporting/Affiliate_Automation',
    file='affiliate_daisycon_submitted_orders',
)

affiliate_cj_staging = RunQueryFromRepoOperator(
    dag=dag,
    task_id='affiliate_cj_staging',
    conn_id='redshift_default',
    directory='11_marketing_reporting/Affiliate_Automation',
    file='affiliate_cj_staging',
)

affiliate_cj_submitted_orders = RunQueryFromRepoOperator(
    dag=dag,
    task_id='affiliate_cj_submitted_orders',
    conn_id='redshift_default',
    directory='11_marketing_reporting/Affiliate_Automation',
    file='affiliate_cj_submitted_orders',
)

affiliate_order_validation = RunQueryFromRepoOperator(
    dag=dag,
    task_id='affiliate_order_validation',
    conn_id='redshift_default',
    directory='11_marketing_reporting/Affiliate_Automation',
    file='affiliate_order_validation',
)

reverse_etl_affiliate_order_validation = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id='reverse_etl_affiliate_order_validation',
    sql="./sql/reverse_etl_affiliate_order_validation.sql"
)

chain(
    cleanup_temp_affiliate_files_s3,
    [fetch_cj_data,
     fetch_everflow_data,
     fetch_daisycon_data],
    [affiliate_cj_staging,
     affiliate_everflow_staging,
     affiliate_daisycon_staging],
    [affiliate_cj_submitted_orders,
     affiliate_everflow_submitted_orders,
     affiliate_daisycon_submitted_orders],
    affiliate_order_validation,
    reverse_etl_affiliate_order_validation
)
