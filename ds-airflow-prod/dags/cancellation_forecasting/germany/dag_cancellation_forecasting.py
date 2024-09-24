import logging
import os
from datetime import datetime, timedelta

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from dags.cancellation_forecasting.germany.cancellation_forecast_process import (
    relativedelta, run_cancellation_forecasts)
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = "sub_cancellation_forecasting_germany"

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ds",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "on_failure_callback": on_failure_callback,
}


def cancellation_forecasting_germany():
    # For now, we should always start to collect data from 2019:
    START_DATE = "2019-01-01"

    # Current date:
    current_month = datetime.strptime(
        str(datetime.today().date().replace(day=1)), "%Y-%m-%d"
    )

    # This is the first prediction date we want to have:
    train_test_cutoff_date = current_month

    # This is where we want to end predictions (usually 12 months time-window):
    end_date = current_month + relativedelta(months=11)

    # This can be different from `train_test_cutoff_date` for training purposes.
    # In production, they coincide, but with a different format:
    month_cutoff = datetime.today().date().replace(day=1)

    local_dir = os.path.dirname(__file__)
    # List with subcategories and encode:
    logging.info("loading subcategory_list_ pkl file to dataframe")
    subcategory_data = pd.read_pickle(os.path.join(local_dir, "subcategory_list_v1.pkl"))

    # Table with the models' information. The algorithm, the features, the data, the parameters:
    logging.info("loading final_subcategory_models_20220726_v3 pkl file to dataframe")
    final_models_table = pd.read_pickle(
        os.path.join(local_dir, "final_subcategory_models_20220726_v3.pkl")
    )

    logging.info("Run the function to obtain the final forecasts")
    final_results = run_cancellation_forecasts(
        start_date=START_DATE,
        train_test_cutoff_date=train_test_cutoff_date,
        end_date=end_date,
        month_cutoff=month_cutoff,
        subcategory_data=subcategory_data,
        final_models_table=final_models_table,
        use_targets=True,
    )

    logging.info("Reduce memory")
    final_results[["cancelled_subscriptions", "cancelled_asv"]] = final_results[
        ["cancelled_subscriptions", "cancelled_asv"]
    ].astype("int32")

    rs = rd.RedshiftSQLHook(redshift_conn_id="redshift")
    redshift_engine = rs.get_sqlalchemy_engine()

    logging.info("Write the results to DB")
    final_results.to_sql(
        "subscription_cancellations_forecasting",
        con=redshift_engine,
        schema="data_science",
        if_exists="append",
        method="multi",
        index=False,
    )


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="Run Subscription Cancellation forecasting model for Germany",
    start_date=datetime(2022, 8, 10),
    schedule_interval=get_schedule("0 6 1 * *"),
    max_active_runs=1,
    catchup=False,
    tags=["Automation", "DS"],
)

t0 = DummyOperator(task_id="start", dag=dag)
cancellation_forecasting_germany = PythonOperator(
    dag=dag,
    task_id="cancellation_forecasting_germany",
    python_callable=cancellation_forecasting_germany,
    executor_config={
        "KubernetesExecutor": {
            "request_memory": "3G",
            "request_cpu": "1500m",
            "limit_memory": "6G",
            "limit_cpu": "2000m",
        }
    },
)

t0 >> cancellation_forecasting_germany
