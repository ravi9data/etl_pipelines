import datetime
import logging
from datetime import timedelta

from airflow import DAG
from airflow.models.baseoperator import chain

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

DAG_ID = 'marketing_conversion_reporting'
REDSHIFT_CONN = 'redshift_default'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 11, 5),
                'retries': 2,
                'retry_delay': timedelta(seconds=15),
                'retry_exponential_backoff': True,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID, default_args=default_args,
          description='DAG to load marketing tables used in reporting',
          schedule_interval=get_schedule('00 8 * * *'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'marketing', 'conversion', 'campaign'])

marketing_conversion_campaign_level_daily_reporting = RunQueryFromRepoOperator(
    dag=dag,
    task_id='marketing_conversion_campaign_level_daily_reporting',
    conn_id='redshift_default',
    directory='11_marketing_reporting',
    file='dm_marketing.marketing_conversion_campaign_level_daily_reporting',
)

crm_list_of_tags = RunQueryFromRepoOperator(
    dag=dag,
    task_id='crm_list_of_tags',
    conn_id='redshift_default',
    directory='11_marketing_reporting',
    file='dm_marketing.crm_list_of_tags',
)

marketing_metrics_by_customer_type_and_mkt_campaigns = RunQueryFromRepoOperator(
    dag=dag,
    task_id='marketing_metrics_by_customer_type_and_mkt_campaigns',
    conn_id='redshift_default',
    directory='11_marketing_reporting',
    file='dm_marketing.marketing_metrics_by_customer_type_and_mkt_campaigns',
)

chain(
    marketing_conversion_campaign_level_daily_reporting,
    crm_list_of_tags,
    marketing_metrics_by_customer_type_and_mkt_campaigns
)
