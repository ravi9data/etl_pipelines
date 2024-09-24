import datetime
import logging

from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator

from dags.data_monitor.shared_tools import get_model_mapping

dag = DAG(
    dag_id="set_model_ids_variable",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="0 23 * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": datetime.timedelta(minutes=10)},
    tags=["monitoring"],
)


def delete_variable() -> None:
    logging.info("Deleting model_ids variable")
    Variable.delete("model_ids")
    logging.info("Done")


def set_model_ids() -> None:
    logging.info("Fetching model_ids")
    model_ids = list(get_model_mapping().keys())
    Variable.set(key="model_ids", value=model_ids)
    logging.info("Fetched {} model ids".format(str(model_ids)))


set_model_ids = PythonOperator(
    dag=dag,
    task_id="set_model_ids",
    python_callable=set_model_ids,
)

delete_model_ids = PythonOperator(
    dag=dag,
    task_id="delete_variable",
    python_callable=delete_variable,
)

delete_model_ids >> set_model_ids
