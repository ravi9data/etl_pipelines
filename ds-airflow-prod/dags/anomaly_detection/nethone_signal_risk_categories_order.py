import logging
from datetime import datetime, timedelta
from re import findall

import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

from plugins.utils.df_tools import optimize_floats, optimize_ints
from plugins.utils.sql import read_sql_file

REDSHIFT_CONN_ID = "redshift"

SCHEMA_NAME = "data_science_dev"
TABLE_NAME = "nethone_signal_risk_categories_order"


def nethone_risk_categories_order():
    pg_hook = PostgresHook(
        postgres_conn_id=REDSHIFT_CONN_ID,
    )
    conn = pg_hook.get_conn()

    # Set start_date to eight days before today
    start_date = datetime.today() - timedelta(days=8)

    # Import 'Nethone' signal data
    QUERY_NETHONE_DATA = read_sql_file(
        "./dags/anomaly_detection/sql/nethone_data.sql"
    ).format(start_date=start_date)
    subscription_nethone_data = pd.read_sql(QUERY_NETHONE_DATA, conn)

    conn.close()

    subscription_nethone_data = optimize_floats(subscription_nethone_data)
    subscription_nethone_data = optimize_ints(subscription_nethone_data)

    subscription_nethone_data = subscription_nethone_data.loc[
        (
            (
                (subscription_nethone_data["shipping_country"].isin(["Germany"]))
                & (subscription_nethone_data["shipping_pc"].str.len().isin([5]))
            )
            | (
                (subscription_nethone_data["shipping_country"].isin(["Austria"]))
                & (subscription_nethone_data["shipping_pc"].str.len().isin([4]))
            )
            | (
                (subscription_nethone_data["shipping_country"].isin(["Netherlands"]))
                & (subscription_nethone_data["shipping_pc"].str.len().isin([7]))
            )
            | (
                (subscription_nethone_data["shipping_country"].isin(["Spain"]))
                & (subscription_nethone_data["shipping_pc"].str.len().isin([5]))
            )
        ),
        ["customer_id", "order_id", "order_submission", "signal_string"],
    ]
    unique_signal_string = subscription_nethone_data["signal_string"].drop_duplicates()

    # Iterate thtrough (JSON) column 'signal_string' and extract its contents via regex
    # Save each individual signal with its respective 'signal_string'
    all_signals_list = []
    for signal_string in unique_signal_string:
        if signal_string is None:
            continue

        if signal_string == "[]":
            results = ["empty_signal"]
            df_signals = pd.DataFrame(
                {"signal_string": signal_string, "signals": results}
            )
        else:
            results = findall(r"\"(.+?)\"", signal_string)
            df_signals = pd.DataFrame(
                {"signal_string": signal_string, "signals": results}
            )
        all_signals_list.append(df_signals)

    all_signals = pd.concat(all_signals_list)
    del unique_signal_string, all_signals_list

    subscription_nethone_data = subscription_nethone_data.merge(
        all_signals, how="left", on="signal_string"
    )
    subscription_nethone_data["signals"] = subscription_nethone_data["signals"].fillna(
        "signals_na"
    )

    pg_hook = PostgresHook(
        postgres_conn_id=REDSHIFT_CONN_ID,
    )
    conn = pg_hook.get_conn()

    # Import 'nethone_signal_risk_categories'
    QUERY_NETHONE_SIGNAL_RISK_CATEGORIES = read_sql_file(
        "./dags/anomaly_detection/sql/nethone_signal_risk_categories.sql"
    )

    signal_risk_categories = pd.read_sql(QUERY_NETHONE_SIGNAL_RISK_CATEGORIES, conn)

    subscription_nethone_data_orders = subscription_nethone_data.merge(
        signal_risk_categories, on="signals", how="left"
    )
    del subscription_nethone_data, signal_risk_categories

    subscription_nethone_data_orders = subscription_nethone_data_orders[
        ["customer_id", "order_id", "signals", "bad_ratio", "signal_risk_category"]
    ].copy()

    # Set missing bad_ratio to negative value to allow proper filtering
    subscription_nethone_data_orders["bad_ratio"] = subscription_nethone_data_orders[
        "bad_ratio"
    ].fillna(-1)

    # Reset index
    subscription_nethone_data_orders.reset_index(drop=True, inplace=True)
    # Determine for each order ID its worst 'bad_ratio'
    subscription_nethone_data_orders = (
        subscription_nethone_data_orders.sort_values(
            by=["bad_ratio"], ascending=False, kind="mergesort"
        )
        .drop_duplicates("order_id")
        .copy()
    )

    # Set 'created_at' column to now()
    subscription_nethone_data_orders["created_at"] = datetime.now()

    engine = PostgresHook(
        postgres_conn_id=REDSHIFT_CONN_ID,
    ).get_sqlalchemy_engine()

    logging.info("Deleting signal_risk_categories_order")

    DELETE_QUERY = read_sql_file(
        "./dags/anomaly_detection/sql/delete_all_table.sql"
    ).format(schema_name=SCHEMA_NAME, table_name=TABLE_NAME)

    engine.connect().execute(DELETE_QUERY)

    logging.info("Writing signal_risk_categories_order")
    subscription_nethone_data_orders.to_sql(
        method="multi",
        name=TABLE_NAME,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists="append",
        index=False,
    )
