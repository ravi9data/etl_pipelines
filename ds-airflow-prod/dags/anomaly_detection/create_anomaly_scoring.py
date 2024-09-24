import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from dags.anomaly_detection.create_asset_risk_categories import \
    create_asset_risk_categories
from dags.anomaly_detection.create_asset_risk_categories_order import \
    create_asset_risk_categories_order
from dags.anomaly_detection.create_payment_risk_categories import \
    create_payment_risk_categories
from dags.anomaly_detection.create_payment_risk_orders import \
    create_payment_risk_orders
from dags.anomaly_detection.nethone_risk_cats import \
    nethon_signal_risk_categories
from dags.anomaly_detection.nethone_signal_risk_categories_order import \
    nethone_risk_categories_order
from plugins.dag_utils import on_failure_callback

default_args = {
    "retries": 2,
    "retry_delay": datetime.timedelta(seconds=5),
}

dag = DAG(
    dag_id="anomaly_scoring",
    default_args=default_args,
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="*/30 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["anomaly"],
)


dag_payment_risk_categories = DAG(
    dag_id="payment_risk_categories",
    default_args=default_args,
    start_date=datetime.datetime(2023, 1, 1),
    schedule_interval="0 5 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["anomaly", "payment_risk"],
    on_failure_callback=on_failure_callback,
)

dag_payment_risk_orders = DAG(
    dag_id="payment_risk_orders",
    default_args=default_args,
    start_date=datetime.datetime(2023, 1, 1),
    schedule_interval="*/30 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["anomaly", "payment_risk"],
    on_failure_callback=on_failure_callback,
)

dag_risk_categories = DAG(
    dag_id="asset_risk_categories",
    default_args=default_args,
    start_date=datetime.datetime(2020, 8, 8),
    schedule_interval="0 22 * * 0",
    max_active_runs=1,
    catchup=False,
    tags=["anomaly", "risk_asset_categories"],
    on_failure_callback=on_failure_callback,
)

dag_risk_categories_order = DAG(
    dag_id="asset_risk_categories_order",
    default_args=default_args,
    start_date=datetime.datetime(2020, 8, 8),
    schedule_interval="*/30 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["anomaly", "risk_asset_categories_order"],
    on_failure_callback=on_failure_callback,
)


dag_nethone_cats = DAG(
    dag_id="nethone_categories",
    default_args=default_args,
    start_date=datetime.datetime(2020, 7, 1),
    schedule_interval="0 12 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["anomaly"],
)


dag_nethone_cat_order = DAG(
    dag_id="nethone_categories_order",
    default_args=default_args,
    start_date=datetime.datetime(2020, 7, 1),
    schedule_interval="*/30 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["anomaly"],
)

nethone_risk_categories_order = PythonOperator(
    dag=dag_nethone_cat_order,
    task_id="nethone_risk_categories_order",
    python_callable=nethone_risk_categories_order,
)

nethone_risk_categories_order = PythonOperator(
    dag=dag_nethone_cats,
    task_id="nethon_signal_risk_categories",
    python_callable=nethon_signal_risk_categories,
    executor_config={
        "KubernetesExecutor": {
            "request_memory": "2Gi",
            "request_cpu": "1200m",
            "limit_memory": "3000Mi",
            "limit_cpu": "2000m",
        }
    },
)

create_payment_risk_categories = PythonOperator(
    dag=dag_payment_risk_categories,
    task_id="payment_risk_categories",
    python_callable=create_payment_risk_categories,
    executor_config={
        "KubernetesExecutor": {
            "request_memory": "4Gi",
            "request_cpu": "1500m",
            "limit_memory": "5Gi",
            "limit_cpu": "2000m",
        }
    },
)

create_payment_risk_orders = PythonOperator(
    dag=dag_payment_risk_orders,
    task_id="payment_risk_orders",
    python_callable=create_payment_risk_orders,
    executor_config={
        "KubernetesExecutor": {
            "request_memory": "2500Mi",
            "request_cpu": "1500m",
            "limit_memory": "3Gi",
            "limit_cpu": "2000m",
        }
    },
)


create_risk_categories = PythonOperator(
    dag=dag_risk_categories,
    task_id="create_risk_categories",
    python_callable=create_asset_risk_categories,
    executor_config={
        "KubernetesExecutor": {
            "request_memory": "4.5Gi",
            "request_cpu": "1500m",
            "limit_memory": "5.5Gi",
            "limit_cpu": "2000m",
        }
    },
)

create_risk_categories = PythonOperator(
    dag=dag_risk_categories_order,
    task_id="create_risk_categories_order",
    python_callable=create_asset_risk_categories_order,
    executor_config={
        "KubernetesExecutor": {
            "request_memory": "4000Mi",
            "request_cpu": "2000m",
            "limit_memory": "5000Mi",
            "limit_cpu": "2000m",
        }
    },
)
