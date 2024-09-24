import logging
from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from plugins.dag_utils import on_failure_callback

REDSHIFT_CONN = 'redshift_default'

DAG_ID = 'luxco_data_archival'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime(2023, 2, 21),
                'retries': 2,
                'on_failure_callback': on_failure_callback
                }

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='DAG to Run Luxco pipeline',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID, 'luxco'],
)

luxco_data_archival = SQLExecuteQueryOperator(
    dag=dag,
    task_id='luxco_data_archival',
    conn_id=REDSHIFT_CONN,
    sql='./sql/data_archival.sql',
    autocommit=True,
    split_statements=True
)
