import datetime
import logging

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator

from plugins.dag_utils import on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator

DAG_ID = 'asset_reconciliation_pipeline'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 3,
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=None,
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'finance', 'assets', 'reconciliation'])

import_data = AirbyteTriggerSyncOperator(
    task_id='import_data_from_gsheet',
    airbyte_conn_id='airbyte_prod',
    connection_id='395808fa-e705-4651-b91a-88b504cdf430',
    asynchronous=False,
    timeout=3600,
    wait_seconds=60,
    dag=dag
)

stage_data = RunQueryFromRepoOperator(
    dag=dag,
    task_id='load_data_to_historical_tbl',
    conn_id='redshift_default',
    directory='13_finance_reporting/Asset Reconciliation',
    file='finance.asset_reconciliation_sap_invoice_import_historical',
    autocommit=True
)

asset_reconciliation_output = RunQueryFromRepoOperator(
    dag=dag,
    task_id='asset_reconciliation_output',
    conn_id='redshift_default',
    directory='13_finance_reporting/Asset Reconciliation',
    file='finance.asset_reconciliation_output',
    autocommit=True
)

asset_reconciliation_quantity_monitoring = RunQueryFromRepoOperator(
    dag=dag,
    task_id='asset_reconciliation_quantity_monitoring',
    conn_id='redshift_default',
    directory='13_finance_reporting/Asset Reconciliation',
    file='finance.asset_reconciliation_quantity_monitoring',
    autocommit=True
)

upsert_asset_recon_qty_monitoring = RunQueryFromRepoOperator(
    dag=dag,
    task_id='upsert_asset_recon_qty_monitoring',
    conn_id='redshift_default',
    directory='13_finance_reporting/Asset Reconciliation',
    file='Upsert finance.asset_reconciliation_quantity_monitoring',
    autocommit=True
)

asset_reconciliation_amount_monitoring = RunQueryFromRepoOperator(
    dag=dag,
    task_id='asset_reconciliation_amount_monitoring',
    conn_id='redshift_default',
    directory='13_finance_reporting/Asset Reconciliation',
    file='finance.asset_reconciliation_amount_monitoring',
    autocommit=True
)

upsert_asset_recon_amt_monitoring = RunQueryFromRepoOperator(
    dag=dag,
    task_id='upsert_asset_recon_amt_monitoring',
    conn_id='redshift_default',
    directory='13_finance_reporting/Asset Reconciliation',
    file='Upsert finance.asset_reconciliation_amount_monitoring',
    autocommit=True
)

upsert_duplicates = RunQueryFromRepoOperator(
    dag=dag,
    task_id='upsert_duplicates',
    conn_id='redshift_default',
    directory='13_finance_reporting/Asset Reconciliation',
    file='Upsert duplicates',
    autocommit=True
)

import_data >> stage_data >> asset_reconciliation_output \
    >> asset_reconciliation_quantity_monitoring >> upsert_asset_recon_qty_monitoring \
    >> asset_reconciliation_amount_monitoring >> upsert_asset_recon_amt_monitoring \
    >> upsert_duplicates
