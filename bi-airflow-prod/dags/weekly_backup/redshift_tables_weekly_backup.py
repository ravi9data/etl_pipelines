import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.redshift.redshift_unload_operator import \
    RedshiftUnloadOperator

DAG_ID = 'redshift_tables_weekly_backup'
REDSHIFT_CONN = 'redshift_default'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng', 'depends_on_past': False,
                'start_date': datetime.datetime(2022, 8, 3), 'retries': 2,
                'retry_delay': datetime.timedelta(seconds=15), 'retry_exponential_backoff': True,
                'on_failure_callback': on_failure_callback}

config = Variable.get('backup_tables_config',
                      default_var=[{"table_schema": "master",
                                    "table_name": "asset_historical",
                                    "unload_task_id": "master_asset_historical_unload"}],
                      deserialize_json=True)

with DAG(DAG_ID, default_args=default_args,
         description='A DAG to unload historical tables to S3 for backup',
         schedule_interval=get_schedule('0 9 * * SUN'),
         max_active_runs=1, catchup=False,
         tags=[DAG_ID, 'backup', 'redshift']) as dag:
    t0 = EmptyOperator(task_id='start')

    with TaskGroup('backup_tasks_group', prefix_group_id=False) as backup_tasks_group:
        for record in config:
            sql_query = 'select * from {0}.{1}'.format(record['table_schema'], record['table_name'])
            s3_prefix = '{0}/{1}/{2}/unload_'.format(record['table_schema'], record['table_name'],
                                                     datetime.datetime.now().strftime("%Y%m%d"))

            unload_sql = RedshiftUnloadOperator(dag=dag, task_id=record['unload_task_id'],
                                                execution_timeout=datetime.timedelta(minutes=20),
                                                redshift_conn_id=REDSHIFT_CONN,
                                                select_query=sql_query,
                                                iam_role='bi-redshift-s3-access',
                                                s3_bucket='grover-bi-redshift-tables-weekly-backup',
                                                s3_prefix=s3_prefix,
                                                unload_options=['PARALLEL TRUE', 'PARQUET'])

    t0 >> backup_tasks_group
