from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from dags.pipeline.monthly_load.config import monthly_load_pipeline_config
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator


def choose_task_group(**kwargs):
    day = datetime.now().day
    if day == 1:
        return 'group_1'
    elif day == 2:
        return 'group_2'
    else:
        return 'end'

def run_group_1():
    print("Running Group 1 tasks")

def run_group_2():
    print("Running Group 2 tasks")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'conditional_taskgroup_dag',
    default_args=default_args,
    description='DAG with conditional TaskGroups based on day of the month',
    schedule_interval='@daily',
)

start = DummyOperator(task_id='start', dag=dag)

branch = BranchPythonOperator(
    task_id='branch',
    python_callable=choose_task_group,
    provide_context=True,
    dag=dag,
)

# TaskGroup for the 1st and 15th of the month
with TaskGroup('group_1', dag=dag) as group_1:
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=run_group_1,
        dag=dag,
    )
    task_1

    prev_task = None
    with TaskGroup('dwh_finance_monthly', prefix_group_id=False, dag=dag) as dwh_finance_monthly:
        group = monthly_load_pipeline_config['dwh_finance_monthly']
        for script in group['script_name']:
            run_sql = RunQueryFromRepoOperator(
                dag=dag,
                task_id=script,
                conn_id='redshift_default',
                directory=group['directory'],
                file=script,
                trigger_rule='all_done'
            )
            if prev_task:
                prev_task >> run_sql
            prev_task = run_sql


# TaskGroup for the 2nd and 16th of the month
with TaskGroup('group_2', dag=dag) as group_2:
    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=run_group_2,
        dag=dag,
    )
    task_2

end = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
start >> branch
branch >> group_1
branch >> group_2
group_1 >> end
group_2 >> end
