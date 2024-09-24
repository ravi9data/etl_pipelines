import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.payment_operations.export_payment_recon_data import \
    export_recon_data
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.redshift.redshift_unload_operator import \
    RedshiftUnloadOperator

DAG_ID = 'export_payment_recon_file'
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

default_args = {
    'owner': 'bi',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 10, 12),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=5)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Drop Payment Reconciliation file to Actuals SFTP ',
    schedule_interval=get_schedule('35 10 * * *'),
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=30),
    tags=['Automation', 'Payment', 'Reconciliation']
)

unload_recon_to_s3 = RedshiftUnloadOperator(
    dag=dag,
    task_id="unload_payment_recon_data_to_s3",
    execution_timeout=datetime.timedelta(minutes=20),
    redshift_conn_id=REDSHIFT_CONN,
    select_query="SELECT * FROM dm_payments.v_reconciliation_actuals",
    iam_role="{{var.value.redshift_iam_role}}",
    s3_bucket="{{var.value.s3_bucket_curated_bi}}",
    s3_prefix="staging/actuals/unload_recon.csv",
    unload_options=["PARALLEL FALSE", "ALLOWOVERWRITE", "DELIMITER ';'",  # "ADDQUOTES",
                    "HEADER", "GZIP"]
)

unload_ixopay_to_s3 = RedshiftUnloadOperator(
    dag=dag,
    task_id="unload_ixopay_data_to_s3",
    execution_timeout=datetime.timedelta(minutes=20),
    redshift_conn_id=REDSHIFT_CONN,
    select_query="SELECT * FROM dm_payments.v_ixopay_actuals",
    iam_role="{{var.value.redshift_iam_role}}",
    s3_bucket="{{var.value.s3_bucket_curated_bi}}",
    s3_prefix="staging/actuals/unload_ixo.csv",
    unload_options=["PARALLEL FALSE", "ALLOWOVERWRITE", "DELIMITER ';'",  # "ADDQUOTES",
                    "HEADER", "GZIP"]
)

export_to_sftp = PythonOperator(
    dag=dag,
    task_id='export_to_sftp',
    python_callable=export_recon_data,
    executor_config=EXECUTOR_CONFIG
)

unload_recon_to_s3 >> unload_ixopay_to_s3 >> export_to_sftp
