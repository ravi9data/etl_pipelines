import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from business_logic.janitor.airflow import db_cleanup
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'airflow_db_cleanup'

EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '8G',
        'request_cpu': '2000m',
        'limit_memory': '8G',
        'limit_cpu': '2000m'
    }
}

# Whether the job should delete the db entries or not. Included if you want to
# temporarily avoid deleting the db entries.
ENABLE_DELETE = True

# Prints the database entries which will be getting deleted
# set to False to avoid printing large lists and slowdown process
PRINT_DELETES = True

# it's possible to configure the retention days setting up the Variable max_db_entry_age_in_days
# or passing max_db_entry_age_in_days when invoking the dag


default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 3, 23),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=30)
}


dag = DAG(
    DAG_ID,
    default_args=default_args,
    max_active_runs=1,
    schedule_interval=get_schedule('0 2 * * *'),
    tags=[DAG_ID, 'maintenance_dag', 'metadata_db_cleaner']
)


print_configuration = PythonOperator(
    dag=dag,
    task_id='print_configuration',
    python_callable=db_cleanup.print_configuration_function,
    op_kwargs={'enable_delete': ENABLE_DELETE, 'print_deletes': PRINT_DELETES}
    )


for db_object in db_cleanup.DATABASE_OBJECTS:
    cleanup_op = PythonOperator(
        dag=dag,
        task_id=f'cleanup_{str(db_object["airflow_db_model"].__name__)}',
        python_callable=db_cleanup.cleanup_function,
        params=db_object,
        op_kwargs={'enable_delete': ENABLE_DELETE, 'print_deletes': PRINT_DELETES},
        executor_config=EXECUTOR_CONFIG
    )

    print_configuration.set_downstream(cleanup_op)
