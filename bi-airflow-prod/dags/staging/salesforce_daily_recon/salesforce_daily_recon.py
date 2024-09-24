import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from dags.staging.salesforce_daily_recon.subscription_alert import (
    subscription_history_recon_alert, subscription_recon_alert)
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'Subscription_SalesForce_Full_Refresh'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 3,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('0 21 * * *'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'salesforce', 'subscription', 'full', 'load'])

stage_sf_history_refresh_data = AirbyteTriggerSyncOperator(
    task_id='stage_sf_history_refresh_data',
    airbyte_conn_id='airbyte_prod',
    connection_id='41548ee7-d0d7-42d2-807b-bd57d3fa9936',
    asynchronous=False,
    timeout=3600,
    wait_seconds=60,
    dag=dag
)

process_update_redshift_table = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="process_update_redshift_table",
    sql="./sql/subscription_payment_refresh.sql"
)
subscription_recon_alert = PythonOperator(
    dag=dag,
    task_id='subscription_recon_alert',
    python_callable=subscription_recon_alert,
)
process_redshift_history_table = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="process_update_redshift_history_table",
    sql="./sql/subscription_payment_histroy_rec.sql"
)
subscription_his_recon_alert = PythonOperator(
    dag=dag,
    task_id='subscription_history_recon_alert',
    python_callable=subscription_history_recon_alert,
)

stage_sf_history_refresh = AirbyteTriggerSyncOperator(
    task_id='stage_sf_Asset_history_refresh',
    airbyte_conn_id='airbyte_prod',
    connection_id='691fbc5e-6208-419e-b8fb-98c0e9d9d0e2',
    asynchronous=False,
    timeout=3600,
    wait_seconds=60,
    dag=dag
)
process_update_salesforce_hist_table = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="process_update_asset_history_table",
    sql="./sql/salesforce_histroy_rec.sql"
)
process_update_sf_table = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="asset_refresh",
    sql="./sql/salesforce_refresh.sql"
)

subscription_recon_alert >> process_update_redshift_table
stage_sf_history_refresh_data >> process_redshift_history_table >> subscription_his_recon_alert
stage_sf_history_refresh >> process_update_salesforce_hist_table >> process_update_sf_table
