import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from plugins.dag_utils import on_failure_callback

REDSHIFT_CONN = 'redshift_default'

first_day_of_month = datetime.today().replace(day=1)
last_day_of_prev_month = first_day_of_month - timedelta(days=1)
date_for_depreciation = last_day_of_prev_month.replace(day=1)
tbl_suffix = date_for_depreciation.strftime('%Y%m')

DAG_ID = 'luxco_final_steps'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime(2022, 10, 20),
                'retries': 2,
                'on_failure_callback': on_failure_callback
                }


def get_db_hook():
    return RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN)


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='DAG to Run Luxco pipeline',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID, 'luxco'],
)

asset_historical_update = SQLExecuteQueryOperator(
    dag=dag,
    task_id='asset_historical_update',
    conn_id=REDSHIFT_CONN,
    sql='./sql/asset_historical_update.sql',
    autocommit=True,
    split_statements=True,
    params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
            "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
            "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
            "tbl_suffix": tbl_suffix}
)

union_source_historical_update = SQLExecuteQueryOperator(
    dag=dag,
    task_id='union_source_historical_update',
    conn_id=REDSHIFT_CONN,
    sql='./sql/union_source_historical_update.sql',
    autocommit=True,
    split_statements=True,
    params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
            "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
            "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
            "tbl_suffix": tbl_suffix}
)

asset_historical_update >> union_source_historical_update
