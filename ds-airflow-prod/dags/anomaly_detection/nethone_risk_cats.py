import logging
from datetime import datetime
from re import findall

import numpy as np
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from dateutil.relativedelta import relativedelta
from scipy.stats import median_absolute_deviation

from plugins.utils.sql import read_sql_file

REDSHIFT_CONN_ID = "redshift"

SCHEMA_NAME = "data_science_dev"
SIGNAL_HIST_TABLE = "nethone_signal_customer_history"
RISK_CAT_TABLE = "nethone_signal_risk_categories"


def nethon_signal_risk_categories():
    pg_hook = PostgresHook(
        postgres_conn_id=REDSHIFT_CONN_ID,
    )
    conn = pg_hook.get_conn()

    # Two years back, rounded back to the 1st of that month
    start_date = (datetime.today() - relativedelta(years=2)).replace(day=1).date()

    # Import 'Nethone' signal data
    QUERY_NETHONE_DATA = read_sql_file(
        "./dags/anomaly_detection/sql/nethone_data.sql"
    ).format(start_date=start_date)
    nethone_data = pd.read_sql(QUERY_NETHONE_DATA, conn)

    nethone_data = nethone_data.loc[
        (
            (
                (nethone_data["shipping_country"].isin(["Germany"]))
                & (nethone_data["shipping_pc"].str.len().isin([5]))
            )
            | (
                (nethone_data["shipping_country"].isin(["Austria"]))
                & (nethone_data["shipping_pc"].str.len().isin([4]))
            )
            | (
                (nethone_data["shipping_country"].isin(["Netherlands"]))
                & (nethone_data["shipping_pc"].str.len().isin([7]))
            )
            | (
                (nethone_data["shipping_country"].isin(["Spain"]))
                & (nethone_data["shipping_pc"].str.len().isin([5]))
            )
        ),
        ["customer_id", "order_id", "order_submission", "signal_string"],
    ]

    unique_signal_string = nethone_data["signal_string"].drop_duplicates()

    # Iterate thtrough (JSON) column 'signal_string' and extract its contents via regex
    # Save each individual signal with its respective 'signal_string'
    all_signals_list = []
    for signal_string in unique_signal_string:
        if signal_string is None:
            continue
        elif signal_string == "[]":
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

    nethone_data = nethone_data.merge(all_signals, how="left", on="signal_string")
    nethone_data["signals"] = nethone_data["signals"].fillna("signals_na")

    QUERY_LABELS = read_sql_file("./dags/anomaly_detection/sql/spectrum_labels3.sql")

    labels = pd.read_sql(QUERY_LABELS, conn)

    labels["is_blacklisted"] = labels["is_blacklisted"] == "True"
    # Set 'blacklisted'
    labels["label"] = np.where(labels["is_blacklisted"], "blacklisted", labels["label"])
    # Use 'label' column values for 'is_fraud' column
    labels["is_fraud"] = np.where(
        labels["label"].isin(["good", "uncertain"]), "False", np.nan
    )
    labels["is_fraud"] = np.where(
        labels["label"].isin(["fraud", "blacklisted"]), "True", labels["is_fraud"]
    )

    nethone_data["customer_id"] = nethone_data["customer_id"].astype(str)

    # Add 'is_fraud' column to 'nethone_data'
    nethone_data = pd.merge(
        nethone_data,
        labels[["customer_id", "is_fraud"]],
        how="left",
        on=["customer_id"],
    )
    del labels

    # Determine all unique customers and all 'bad' customers
    unique_customers = (
        nethone_data[~nethone_data["is_fraud"].isna()]
        .groupby(by="signals")
        .agg({"customer_id": "nunique"})
        .rename(columns={"customer_id": "unique_customers"})
    )
    unique_bad_customers = (
        nethone_data[nethone_data["is_fraud"].isin(["True"])]
        .groupby(by="signals")
        .agg({"customer_id": "nunique"})
        .rename(columns={"customer_id": "unique_bad_customers"})
    )

    # Create stats for the signals (counting multiple different signals for a single customer)
    multiple_signals_stats_no_na = pd.merge(
        unique_customers, unique_bad_customers, how="outer", on=["signals"]
    )
    multiple_signals_stats_no_na["unique_bad_customers"] = multiple_signals_stats_no_na[
        "unique_bad_customers"
    ].fillna(0)
    multiple_signals_stats_no_na["bad_ratio"] = (
        multiple_signals_stats_no_na["unique_bad_customers"]
        / multiple_signals_stats_no_na["unique_customers"]
    )

    # Add statistics from 'multiple_signals_stats_no_na' to 'nethone_data'
    # Filter out rows NA in 'bad_ratio' and 'is_fraud'
    single_signals_no_na = pd.merge(
        nethone_data, multiple_signals_stats_no_na, how="left", on=["signals"]
    )
    del multiple_signals_stats_no_na
    single_signals_no_na = single_signals_no_na[
        ~single_signals_no_na["bad_ratio"].isna()
    ]
    single_signals_no_na = single_signals_no_na[
        ~single_signals_no_na["is_fraud"].isna()
    ]
    # Give each customer ID its worst 'bad_ratio'
    single_signals_no_na.reset_index(drop=True, inplace=True)
    single_signals_no_na = single_signals_no_na.sort_values(
        by=["bad_ratio"], ascending=False, kind="mergesort"
    ).drop_duplicates("customer_id")

    # Create stats for the signals (only the 'worst' signal of each customer is counted)
    unique_customers = (
        single_signals_no_na.groupby(by="signals")
        .agg({"customer_id": "nunique"})
        .rename(columns={"customer_id": "unique_customers"})
    )
    unique_bad_customers = (
        single_signals_no_na[single_signals_no_na["is_fraud"] == "True"]
        .groupby(by="signals")
        .agg({"customer_id": "nunique"})
        .rename(columns={"customer_id": "unique_bad_customers"})
    )
    single_signals_stats_no_na = pd.merge(
        unique_customers, unique_bad_customers, how="outer", on=["signals"]
    )
    single_signals_stats_no_na["unique_bad_customers"] = single_signals_stats_no_na[
        "unique_bad_customers"
    ].fillna(0)
    single_signals_stats_no_na["bad_ratio"] = (
        single_signals_stats_no_na["unique_bad_customers"]
        / single_signals_stats_no_na["unique_customers"]
    )

    # Determine median and median_absolute_deviation (mad) from 'single_signals_no_na'
    stats_bad_ratio = single_signals_no_na["bad_ratio"].agg(
        {np.median, median_absolute_deviation}
    )

    # Add 'median_bad_ratio' and 'mad' to stats
    signal_risk_categories = single_signals_stats_no_na.reset_index()
    signal_risk_categories["median_bad_ratio"] = stats_bad_ratio["median"]
    signal_risk_categories["mad"] = stats_bad_ratio["median_absolute_deviation"]

    # Set 'signal_risk_category' based on each signal's 'bad_ratio', median_bad_ratio & 'mad'
    signal_risk_categories["signal_risk_category"] = None
    signal_risk_categories["signal_risk_category"] = np.where(
        (signal_risk_categories["signal_risk_category"].isna())
        & (
            signal_risk_categories["bad_ratio"]
            >= (
                signal_risk_categories["median_bad_ratio"]
                + 3 * signal_risk_categories["mad"]
            )
        ),
        "highest_risk",
        signal_risk_categories["signal_risk_category"],
    )
    signal_risk_categories["signal_risk_category"] = np.where(
        (signal_risk_categories["signal_risk_category"].isna())
        & (
            signal_risk_categories["bad_ratio"]
            >= (
                signal_risk_categories["median_bad_ratio"]
                + 0.5 * signal_risk_categories["mad"]
            )
        ),
        "higher_risk",
        signal_risk_categories["signal_risk_category"],
    )
    signal_risk_categories["signal_risk_category"] = np.where(
        (signal_risk_categories["signal_risk_category"].isna())
        & (
            signal_risk_categories["bad_ratio"]
            > signal_risk_categories["median_bad_ratio"]
        ),
        "above_average_risk",
        signal_risk_categories["signal_risk_category"],
    )
    signal_risk_categories["signal_risk_category"] = np.where(
        (signal_risk_categories["signal_risk_category"].isna())
        & (
            signal_risk_categories["bad_ratio"]
            < signal_risk_categories["median_bad_ratio"]
            - (1 * signal_risk_categories["mad"])
        ),
        "below_average_risk",
        signal_risk_categories["signal_risk_category"],
    )
    signal_risk_categories["signal_risk_category"] = np.where(
        signal_risk_categories["signal_risk_category"].isna(),
        "regular_risk",
        signal_risk_categories["signal_risk_category"],
    )
    signal_risk_categories["signal_risk_category"] = np.where(
        signal_risk_categories["unique_customers"] <= 15,
        "unknown_risk",
        signal_risk_categories["signal_risk_category"],
    )
    signal_risk_categories["signal_risk_category"] = np.where(
        signal_risk_categories["bad_ratio"] < 0.0001,
        "no_fraud_yet",
        signal_risk_categories["signal_risk_category"],
    )

    # Add more information to the column 'signal_risk_category'
    # Final result wil be (example) "63.0% | 17 - 29 | highest_risk"
    signal_risk_categories["unique_good_customers"] = (
        signal_risk_categories["unique_customers"]
        - signal_risk_categories["unique_bad_customers"]
    )
    signal_risk_categories["good_fraud"] = (
        signal_risk_categories["unique_good_customers"].astype(str)
        + " - "
        + signal_risk_categories["unique_bad_customers"].astype(str)
    )
    signal_risk_categories["good_fraud"] = signal_risk_categories[
        "good_fraud"
    ].str.replace(r"\.0", "", regex=True)
    signal_risk_categories["ratio"] = pd.Series(
        ["{0:.1f}%".format(val * 100) for val in signal_risk_categories["bad_ratio"]],
        index=signal_risk_categories.index,
    )
    signal_risk_categories["nethone_info"] = (
        signal_risk_categories["ratio"]
        + " | "
        + signal_risk_categories["good_fraud"]
        + " | "
        + signal_risk_categories["signal_risk_category"]
    )
    signal_risk_categories["signal_risk_category"] = signal_risk_categories[
        "nethone_info"
    ]

    signal_risk_categories = signal_risk_categories[
        ["signals", "unique_customers", "bad_ratio", "signal_risk_category"]
    ].sort_values(["bad_ratio", "unique_customers"], ascending=False)

    # Set 'created_at' column to now()
    signal_risk_categories = signal_risk_categories.loc[
        :, ["signals", "bad_ratio", "signal_risk_category"]
    ]
    signal_risk_categories["created_at"] = datetime.now()
    # Round
    signal_risk_categories = signal_risk_categories.round(3)

    engine = PostgresHook(
        postgres_conn_id=REDSHIFT_CONN_ID,
    ).get_sqlalchemy_engine()

    logging.info("Deleting signal_risk_categories_order")

    DELETE_QUERY = read_sql_file(
        "./dags/anomaly_detection/sql/delete_all_table.sql"
    ).format(schema_name=SCHEMA_NAME, table_name=RISK_CAT_TABLE)

    engine.connect().execute(DELETE_QUERY)

    logging.info("Writing signal_risk_categories")

    signal_risk_categories.to_sql(
        method="multi",
        name=RISK_CAT_TABLE,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists="append",
        index=False,
        chunksize=5000,
    )

    order_worst_nethone_signal = nethone_data[
        ["customer_id", "order_id", "order_submission", "signals"]
    ].merge(signal_risk_categories[["signals", "bad_ratio"]], on="signals")

    order_worst_nethone_signal.reset_index(drop=True, inplace=True)
    order_worst_nethone_signal = (
        order_worst_nethone_signal.sort_values(
            by=["bad_ratio"], ascending=False, kind="mergesort"
        )
        .drop_duplicates("order_id")
        .copy()
    )
    order_worst_nethone_signal = order_worst_nethone_signal.rename(
        columns={"signals": "worst_signal"}
    )

    order_worst_nethone_signal["customer_id"] = order_worst_nethone_signal[
        "customer_id"
    ].astype(int)

    logging.info("Deleting signal_risk_categories_order")

    DELETE_QUERY = read_sql_file(
        "./dags/anomaly_detection/sql/delete_all_table.sql"
    ).format(schema_name=SCHEMA_NAME, table_name=SIGNAL_HIST_TABLE)

    engine.connect().execute(DELETE_QUERY)

    logging.info("Writing order_worst_nethone_signal")

    order_worst_nethone_signal.to_sql(
        method="multi",
        name=SIGNAL_HIST_TABLE,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists="append",
        index=False,
        chunksize=5000,
    )

    logging.info("Done")
