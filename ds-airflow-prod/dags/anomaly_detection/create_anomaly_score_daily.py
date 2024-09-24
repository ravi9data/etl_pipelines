import logging
from typing import Any

import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

from plugins.utils.sql import read_sql_file

CONN_ID = "redshift"
SCHEMA_NAME = "data_science_dev"
TABLE_PAYMENT_RISK_CATS = "payment_risk_categories"
TABLE_ANOMALY_SCORE_ORDER = "anomaly_score_order"


# This function can be generalized across other dags, too
def get_engine(connection_id: str) -> Any:
    return PostgresHook(
        postgres_conn_id=connection_id,
    ).get_sqlalchemy_engine()


def create_anomaly_scoring() -> None:
    # Fill table 'fraud_and_credit_risk.payment_risk_categories'
    order_id_payment_risk_categories = pd.read_csv(
        "order_id_payment_risk_categories.csv", index_col=0
    )
    order_id_payment_risk_categories["created_at"] = pd.Timestamp.now(tz=None)

    engine = get_engine(CONN_ID)

    logging.info("Deleting {}".format(TABLE_PAYMENT_RISK_CATS))

    DELETE_QUERY = read_sql_file(
        "./dags/anomaly_detection/sql/delete_all_table.sql"
    ).format(schema_name=SCHEMA_NAME, table_name=TABLE_PAYMENT_RISK_CATS)

    engine.connect().execute(DELETE_QUERY)

    logging.info("Writing {}".format(TABLE_PAYMENT_RISK_CATS))

    order_id_payment_risk_categories.to_sql(
        method="multi",
        name=TABLE_PAYMENT_RISK_CATS,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists="append",
        index=False,
    )

    # Import data from csv files created by the R scripts
    # Germany
    anomaly_score_order_germany = pd.read_csv(
        "anomaly_score_order_germany.csv", index_col=0
    )
    # Austria
    anomaly_score_order_austria = pd.read_csv(
        "anomaly_score_order_austria.csv", index_col=0
    )
    # Netherlands (NL)
    anomaly_score_order_NL = pd.read_csv("anomaly_score_order_NL.csv", index_col=0)
    # Spain
    anomaly_score_order_spain = pd.read_csv(
        "anomaly_score_order_spain.csv", index_col=0
    )

    # Combine DFs from all countries
    anomaly_score_order = anomaly_score_order_germany.append(
        anomaly_score_order_austria
    )
    anomaly_score_order = anomaly_score_order.append(anomaly_score_order_NL)
    anomaly_score_order = anomaly_score_order.append(anomaly_score_order_spain)

    # Round all numeric column to the first digit
    anomaly_score_order = anomaly_score_order.round(1)

    logging.info("Deleting {}".format(TABLE_ANOMALY_SCORE_ORDER))

    DELETE_QUERY = read_sql_file(
        "./dags/anomaly_detection/sql/delete_all_table.sql"
    ).format(schema_name=SCHEMA_NAME, table_name=TABLE_ANOMALY_SCORE_ORDER)

    engine.connect().execute(DELETE_QUERY)

    logging.info("Writing {}".format(TABLE_ANOMALY_SCORE_ORDER))

    anomaly_score_order.to_sql(
        method="multi",
        name=TABLE_ANOMALY_SCORE_ORDER,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists="append",
        index=False,
    )
