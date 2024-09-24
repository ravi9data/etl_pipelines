import datetime
import logging

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import \
    KubernetesPodOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from kubernetes.client import models as k8s

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.k8s import data_nodes

logger = logging.getLogger(__name__)

DAG_ID = 'order_report_BF'

default_args = {
    'owner': 'bi',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 11, 8),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Live order report BF',
    schedule_interval=get_schedule('*/30 * * * *'),
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=30),
    tags=['order_live_report', 'dbt', 'BF', DAG_ID]
)

dbt_run_example = KubernetesPodOperator(
    dag=dag,
    task_id="dbt_order_live_report_BF",
    namespace="airflow-bi",
    in_cluster=True,
    name="order_live_report_BF",
    image="031440329442.dkr.ecr.eu-central-1.amazonaws.com/bi-dbt:main-latest",
    cmds=["dbt", "run"],
    arguments=["--models", "+BF"],
    labels={"dbt": "run", "models": "order_live_report_BF"},
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

staging_refresh = SQLExecuteQueryOperator(
    dag=dag,
    task_id='staging_refresh',
    conn_id='redshift_default',
    sql='./sql/staging_data_refresh.sql',
    autocommit=False,
    split_statements=True
    )

staging_refresh >> dbt_run_example
