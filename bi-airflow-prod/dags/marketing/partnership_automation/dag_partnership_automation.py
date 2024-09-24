import datetime
import logging
import os

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator

from dags.marketing.partnership_automation.get_everflow_partnership_orders import \
    get_everflow_orders
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

DAG_ID = 'partnership_automation'
REDSHIFT_CONN = 'redshift_default'

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
                'retries': 1,
                'retry_delay': datetime.timedelta(seconds=30),
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('15 8,9 * * 1-7'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'partnership', 'partnership_automation'])

fetch_everflow_data = PythonOperator(
    dag=dag,
    task_id="fetch_everflow_data",
    python_callable=get_everflow_orders,
    op_kwargs={"start_date": start_date, "end_date": end_date, "page_size": 500}
)

partnership_everflow_staging = RunQueryFromRepoOperator(
    dag=dag,
    task_id='partnership_everflow_staging',
    conn_id='redshift_default',
    directory='11_marketing_reporting/Partnership_Automation',
    file='partnership_everflow_staging',
)

partnership_everflow_submitted_orders = RunQueryFromRepoOperator(
    dag=dag,
    task_id='partnership_everflow_submitted_orders',
    conn_id='redshift_default',
    directory='11_marketing_reporting/Partnership_Automation',
    file='partnership_everflow_submitted_orders',
)

partnership_order_validation = RunQueryFromRepoOperator(
    dag=dag,
    task_id='partnership_order_validation',
    conn_id='redshift_default',
    directory='11_marketing_reporting/Partnership_Automation',
    file='partnership_order_validation',
)

chain(
    fetch_everflow_data,
    partnership_everflow_staging,
    partnership_everflow_submitted_orders,
    partnership_order_validation
)
