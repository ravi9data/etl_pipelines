import datetime
import logging

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import \
    KubernetesPodOperator
from kubernetes.client import models as k8s

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.k8s import data_nodes

logger = logging.getLogger(__name__)

DAG_ID = 'ops_report_BF'

default_args = {
    'owner': 'bi',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 11, 17),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Live operations report BF',
    schedule_interval=get_schedule('0 * * * *'),
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=70),
    tags=['ops_live_report', 'dbt', 'BF', DAG_ID]
)

dbt_run_example = KubernetesPodOperator(
    dag=dag,
    task_id="dbt_ops_live_report_BF",
    namespace="airflow-bi",
    in_cluster=True,
    name="ops_live_report_BF",
    image="031440329442.dkr.ecr.eu-central-1.amazonaws.com/bi-dbt:main-latest",
    cmds=["dbt", "run"],
    arguments=["--models", "+BF_ops"],
    labels={"dbt": "run", "models": "ops_live_report_BF"},
    image_pull_policy='Always',
    node_selector=data_nodes.SPOT_NODE_SELECTORS,
    tolerations=data_nodes.TOLERATIONS,
    env_vars=[
        k8s.V1EnvVar(name="REDSHIFT_HOST", value='{{ var.value.redshift_hostname }}'),
        k8s.V1EnvVar(name="DBT_USER", value='{{ var.value.dbt_user }}'),
        # recommended approach to pass sensitive data, fetch from AWS SSM
        # AWS SSM location: /production/data/bi/airflow/config/dbt_password recommended
        k8s.V1EnvVar(name="DBT_PASSWORD", value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(key="dbt_password", name="airflow-ssm-secret"))),
        # not recommended, fetch from airflow variable
        # k8s.V1EnvVar(name="DBT_PASSWORD", value='{{ var.value.dbt_password }}')
    ]
)
