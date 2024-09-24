import datetime
import logging
import os
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import \
    KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
from kubernetes.client import models as k8s

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.example_operator import ExampleOperator
from plugins.utils.k8s import data_nodes

DAG_ID = 'example_dag'

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 6, 9),
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=5),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=5)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Just an example DAG with a custom Operator.',
    schedule_interval=get_schedule('*/15 * * * *'),
    max_active_runs=1,
    catchup=False,
    tags=['example', DAG_ID, 'example_operator']
)


def print_task(**context):
    _task = context['task']
    _dag = context['dag']
    # _ti = context['ti']
    logger.info(f'Executed from {_task.task_id} inside {_dag.dag_id}')


def print_version():
    version = sys.version
    logger.info(version)
    return version


def print_env():
    env = os.environ
    team = env.get('TEAM')
    logger.info(team)


def handle_xcom(**kwargs):
    task_id = 'print_version'
    task_instance = kwargs['ti']
    value = task_instance.xcom_pull(key='return_value', task_ids=task_id)

    logger.info(f'Value returned from {task_id}: {value}')


def print_env_k8s_pod():
    env = os.environ
    team = env.get('TEAM')
    logger.info(team)


task_group = TaskGroup(
    dag=dag,
    group_id='just_a_task_group'
)

task_0 = PythonOperator(
    dag=dag,
    task_id='print_environment',
    python_callable=print_env
)

task_1 = BashOperator(
    dag=dag,
    task_id='print_date',
    bash_command='date'
)

task_2 = PythonOperator(
    dag=dag,
    task_id='print_task',
    python_callable=print_task,
    task_group=task_group
)

task_3 = PythonOperator(
    dag=dag,
    task_id='print_version',
    python_callable=print_version,
    task_group=task_group
)

task_4 = PythonOperator(
    dag=dag,
    task_id='xcom_example',
    python_callable=handle_xcom
)

task_5 = ExampleOperator(
    dag=dag,
    task_id='example_operator',
    example_argument='just a test'
)

task_6 = KubernetesPodOperator(
    dag=dag,
    task_id="k8s_pod_operator",
    namespace="airflow-data-eng",
    in_cluster=True,
    name="k8s_pod_operator",
    image="debian",
    cmds=["bash", "-cx"],
    arguments=["echo", "10"],
    labels={"custom_label": "example"},
    image_pull_policy='Always',
    get_logs=True,
    node_selector=data_nodes.ON_DEMAND_NODE_SELECTORS,
    tolerations=data_nodes.TOLERATIONS,
)

task_7 = PythonOperator(
    dag=dag,
    task_id="print_env_k8s_pod",
    python_callable=print_env_k8s_pod,
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources=k8s.V1ResourceRequirements(
                            limits={"cpu": "250m", "memory": "250M"},
                            requests={"cpu": "250m", "memory": "250M"},
                        ),
                        env_from=[
                            k8s.V1EnvFromSource(
                                secret_ref=k8s.V1SecretEnvSource(
                                    name="airflow-ssm-secret"
                                )
                            )
                        ],
                    )
                ]
            )
        )
    },
)

task_0 >> task_1 >> [task_2, task_3] >> task_4 >> task_5 >> task_6 >> task_7
