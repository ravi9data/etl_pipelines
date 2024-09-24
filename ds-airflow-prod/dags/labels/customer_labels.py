import datetime
import logging

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pandas import DataFrame
from sqlalchemy.engine import Engine

from dags.labels.data_cleansing import clean_data
from dags.labels.model_labels import final_data, model_fit
from dags.labels.query_DB import get_data
from plugins.utils.sql import get_engine, read_sql_file

DAG_ID = "customer_labels3"
CONN_ID = "postgres_write"
SCHEMA_NAME = "order_approval"
TABLE_NAME = "customer_labels3"

RISK_CUSTOMER_CONN_ID = "postgres_risk_customer"
RISK_SCHEMA_NAME = "public"
RISK_TABLE_NAME = "customer_label"

default_args = {
    "retries": 2,
    "retry_delay": datetime.timedelta(seconds=5),
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="45 23 * * *",
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID, "labels"],
)


def create_labels():
    data = get_data()
    x_test = clean_data(data)

    # free up some memory
    data = data[["customer_id", "is_blacklisted"]]
    pred, pred_prob = model_fit(x_test)
    prediction_data = final_data(x_test, pred, pred_prob)
    prediction_data = prediction_data.merge(
        data, on="customer_id", how="left"
    ).set_index("customer_id")

    logging.info("Created prediction data.")

    engine = get_engine(CONN_ID)

    DELETE_QUERY = read_sql_file("./dags/labels/sql/delete_all_table.sql").format(
        schema_name=SCHEMA_NAME,
        table_name=TABLE_NAME,
    )
    engine.execute(DELETE_QUERY)
    logging.info(f"Deleted content of {TABLE_NAME}.")

    prediction_data.to_sql(
        method="multi",
        name=TABLE_NAME,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists="append",
        index_label="customer_id",
        chunksize=1000
    )

    logging.info(f"Insert to {SCHEMA_NAME}.{TABLE_NAME} is successful.")

    risk_customer_engine: Engine = get_engine(RISK_CUSTOMER_CONN_ID)

    risk_customer_df: DataFrame = prediction_data.filter(["customer_id", "label"])

    risk_customer_df["updated_at"] = datetime.datetime.now()

    risk_customer_df["updated_at"] = risk_customer_df["updated_at"].apply(
        lambda x: x.isoformat()
    )

    customer_labels = risk_customer_df.reset_index().loc[:].values.tolist()

    parameters_list = []
    for customer_label in customer_labels:
        parameters_list.append(
            f"({customer_label[0]}, '{customer_label[1]}', '{customer_label[2]}')"
        )

    parameters = ", ".join(parameters_list)

    INSERT_ON_CONFLICT = read_sql_file("./dags/labels/sql/insert_on_conflict.sql").format(
        schema_name=RISK_SCHEMA_NAME,
        table_name=RISK_TABLE_NAME,
        parameters=parameters,
    )

    risk_customer_engine.execute(
        INSERT_ON_CONFLICT,
        parameters=parameters
    )

    logging.info(
        f"Insert to {RISK_SCHEMA_NAME}.{RISK_TABLE_NAME} is successful."
    )


create_labels = PythonOperator(
    dag=dag,
    task_id="create_labels",
    python_callable=create_labels,
    executor_config={
        "KubernetesExecutor": {
            "request_memory": "7G",
            "request_cpu": "1500m",
            "limit_memory": "8.5G",
            "limit_cpu": "2000m",
        }
    },
)


insert_state_changes = PostgresOperator(
    dag=dag,
    postgres_conn_id=CONN_ID,
    task_id="insert_state",
    sql="./sql/state_changes.sql",
)

create_labels >> insert_state_changes
