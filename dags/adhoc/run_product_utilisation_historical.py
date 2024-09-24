import datetime
import logging

from airflow import DAG
from airflow.models.baseoperator import chain

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 8, 31),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=30)}

DAG_ID = 'pricing_data_refresh_daily'

with DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval=get_schedule('30 7 * * *'),
        max_active_runs=1,
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=15),
        tags=['Git', 'redshift']) as dag:

    run_product_utilisation_historical_sql = RunQueryFromRepoOperator(
        dag=dag,
        task_id='run_product_utilisation_historical_sql',
        conn_id='redshift_default',
        repo='data-pricing',
        directory='scripts',
        file='product_utilisation_historical'
    )

    run_perc_pp_sql = RunQueryFromRepoOperator(
        dag=dag,
        task_id='run_perc_pp_sql',
        conn_id='redshift_default',
        repo='data-pricing',
        directory='scripts',
        file='perc_pp'
    )

    pricing_vs_subscriptions_acquired_vs_traffic_acquired = RunQueryFromRepoOperator(
        dag=dag,
        task_id='pricing_vs_subscriptions_acquired_vs_traffic_acquired',
        conn_id='redshift_default',
        repo='Redshift-ETL-DA2.0',
        directory='9_SPV/Pricing',
        file='pricing.pricing_vs_subscriptions_acquired_vs_traffic_acquired'
    )

    chain(
        run_product_utilisation_historical_sql,
        run_perc_pp_sql,
        pricing_vs_subscriptions_acquired_vs_traffic_acquired
    )
