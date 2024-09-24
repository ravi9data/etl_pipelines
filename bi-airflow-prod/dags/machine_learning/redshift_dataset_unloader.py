import datetime
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from dags.machine_learning.ml_unload_config import datasets_config
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.redshift.redshift_unload_operator import \
    RedshiftUnloadOperator

DAG_ID = 'redshift_ml_datasets_unloader'
REDSHIFT_CONN = 'redshift_default'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 8, 3),
                'retries': 2,
                'retry_delay': datetime.timedelta(seconds=15),
                'retry_exponential_backoff': True,
                'on_failure_callback': on_failure_callback}

with DAG(DAG_ID,
         default_args=default_args,
         description='A support DAG to unload dataset for ml to S3',
         schedule_interval=get_schedule('30 * * * *'),
         max_active_runs=1,
         dagrun_timeout=datetime.timedelta(minutes=20),
         catchup=False,
         tags=[DAG_ID, 'machine_learning', 'redshift']) as dag:
    t0 = EmptyOperator(task_id='start')

    with TaskGroup('ml_unload_tasks_group', prefix_group_id=False) as ml_unload_tasks_group:
        for record in datasets_config:
            unload_sql = RedshiftUnloadOperator(dag=dag, task_id=record['unload_task_id'],
                                                execution_timeout=datetime.timedelta(minutes=10),
                                                redshift_conn_id=REDSHIFT_CONN,
                                                select_query=record['sql_query'],
                                                iam_role='{{var.value.redshift_iam_role}}',
                                                s3_bucket=record['redshift_datasets_s3_bucket'],
                                                s3_prefix=record['s3_prefix'],
                                                unload_options=record['unload_option'])

    t0 >> ml_unload_tasks_group
