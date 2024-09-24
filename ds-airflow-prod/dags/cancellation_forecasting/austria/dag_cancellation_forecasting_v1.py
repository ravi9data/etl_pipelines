import logging
import os
from datetime import datetime, timedelta

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from dateutil.relativedelta import relativedelta

from dags.cancellation_forecasting.austria.cancellation_forecast_process import \
    run_cancellation_forecasts
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = "sub_cancellation_forecasting_austria_1"

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ds",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "on_failure_callback": on_failure_callback
}


def cancellation_forecasting_austria_1():
    # For now, we should always start to collect data from 2019:
    START_DATE = "2019-11-01"

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
    logging.info("loading subcategory_list csv file to dataframe")
    subcategory_data = pd.read_csv(os.path.join(local_dir, "subcategory_list_at.csv"))

    # Table with the models' information. The algorithm, the features, the data, the parameters:
    logging.info("loading final_models_AT_20221013 pkl file to dataframe")
    final_models_table = pd.read_pickle(os.path.join(local_dir,
                                                     "final_models_AT_20221013.pkl"))

    logging.info("Run the function to obtain the final forecasts")
    final_results = run_cancellation_forecasts(
        start_date=START_DATE,
        train_test_cutoff_date=train_test_cutoff_date,
        end_date=end_date,
        month_cutoff=month_cutoff,
        subcategory_data=subcategory_data,
        final_models_table=final_models_table,
        use_targets=False,
    )

    logging.info("Reduce memory")
    final_results[["cancelled_subscriptions", "cancelled_asv"]] = final_results[
        ["cancelled_subscriptions", "cancelled_asv"]
    ].astype("int32")

    rs = rd.RedshiftSQLHook(redshift_conn_id="redshift")
    redshift_engine = rs.get_sqlalchemy_engine()

    logging.info("Write the results to DB")
    final_results.to_sql(
        "subscription_cancellations_forecasting_1",
        con=redshift_engine,
        schema="data_science",
        if_exists="append",
        method="multi",
        index=False,
    )


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="Run Subscription Cancellation forecasting model for Austria",
    start_date=datetime(2023, 8, 1),
    schedule_interval=get_schedule("45 6 1 * *"),
    max_active_runs=1,
    catchup=False,
    tags=["Automation", "DS"]
)

t0 = DummyOperator(task_id="start", dag=dag)
cancellation_forecasting_austria_1 = PythonOperator(
    dag=dag,
    task_id="cancellation_forecasting_austria_1",
    python_callable=cancellation_forecasting_austria_1,
    executor_config={
        "KubernetesExecutor": {
            "request_memory": "1500Mi",
            "request_cpu": "1200m",
            "limit_memory": "3Gi",
            "limit_cpu": "2000m",
        }
    },
)

t0 >> cancellation_forecasting_austria_1
