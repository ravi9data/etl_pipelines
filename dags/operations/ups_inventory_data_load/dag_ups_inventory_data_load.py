import datetime

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import on_failure_callback
from plugins.slack_utils import send_notification

REDSHIFT_CONN = 'redshift_default'
S3_BUCKET = 'grover-eu-central-1-production-data-bi-curated'
S3_DESTINATION_KEY = "ups_inventory/unload.json"

default_args = {'owner': 'bi-eng', 'depends_on_past': False,
                'start_date': datetime.datetime(2022, 8, 8), 'retries': 2,
                'retry_delay': datetime.timedelta(seconds=15), 'retry_exponential_backoff': True,
                'on_failure_callback': on_failure_callback}


def number_of_inventory_assets():
    rs = rd.RedshiftSQLHook(redshift_conn_id="redshift_default")
    conn = rs.get_conn()
    sql_query = "select count(1) from stg_external_apis.ups_nl_oh_inventory \
where report_date = current_date"

    cr = conn.cursor()
    cr.execute(sql_query)
    row_count = cr.fetchone()[0]
    message = f"UPS EU Inventory Daily Snapshot has been imported successfully " \
              f"for {datetime.datetime.now().strftime('%Y-%m-%d')} updated!"

    if row_count > 0:
        message += "\nNumber of assets reported " + str(row_count) + " in stg_external_" \
                                                                     "apis.ups_nl_oh_inventory."

    send_notification(message)


dag = DAG(
    dag_id="import_ups_inventory_data",
    schedule_interval='45 9 * * *',
    catchup=False,
    tags=['gsheet', 'UPS Inventory'],
    default_args=default_args
)

stage_inventory_data = AirbyteTriggerSyncOperator(
    task_id='airbyte_stage_inventory_data',
    airbyte_conn_id='airbyte_prod',
    connection_id='84a5f9f3-af3d-45f1-9e51-244af5aca31a',
    asynchronous=False,
    timeout=3600,
    wait_seconds=60,
    dag=dag
)

stage_unallocable_data = AirbyteTriggerSyncOperator(
    task_id='airbyte_stage_allocable_data',
    airbyte_conn_id='airbyte_prod',
    connection_id='28e33f5f-6406-45a3-a805-e0092a070331',
    asynchronous=False,
    timeout=3600,
    wait_seconds=60,
    dag=dag
)

process_inventory_data = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="process_inventory_data",
    sql="./sql/stg_ups_nl_oh_inventory.sql"
)

process_unallocable_data = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="process_allocable_data",
    sql="./sql/stg_ups_nl_ib_unallocable.sql"
)

ups_eu_reconciliation = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="ups_eu_reconciliation",
    sql="./sql/ups_eu_reconciliation.sql"
)

ups_eu_inbound_unallocable = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="ups_eu_inbound_unallocable",
    sql="./sql/ups_eu_inbound_unallocable.sql"
)


run_check_inventory_assets = PythonOperator(
    dag=dag,
    task_id='run_check_inventory_assets',
    python_callable=number_of_inventory_assets,
)

stage_inventory_data >> stage_unallocable_data >> process_inventory_data \
     >> process_unallocable_data >> ups_eu_reconciliation >> \
     ups_eu_inbound_unallocable >> run_check_inventory_assets
