import logging
from datetime import datetime

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

# BANK_MAPPING was taken from https://github.com/devsbb/data-trinity v0.70.2
# If further referneces to data-trinity will occur, data-trinity integration
# into image expansion should be made
from dags.anomaly_detection.constants import BANK_MAPPING
from plugins.utils.sql import get_engine, read_sql_file

REDSHIFT_CONN_ID = "redshift"
SCHEMA_NAME = "stg_fraud_and_credit_risk"
TABLE_NAME = "payment_risk_categories"

# Use individual country's data if item_quantity > this value
THRESHOLD_USE_INDIVIDUAL_COUNTRY = 10


def calculate_fraud_ratio(
    df: pd.DataFrame,
    grouping_variables: list,
    label_col: str = "fraud_yn",
    counting_col: str = "item_quantity",
) -> pd.DataFrame:
    """
    Calculate the amount of instances within `label_col` as well as
    the fraud ratio for all `grouping_variables`
    """
    grouping_variables_temp = grouping_variables.copy()
    grouping_variables_temp.append(label_col)

    # We want to keep values for NaN labels but not have them
    # counts towards the sum in `counting_col`
    df.loc[df[label_col].isna(), [counting_col, label_col]] = [0, False]

    aggregation_fraud_ratio = (
        df.groupby(grouping_variables_temp, dropna=False)
        .sum()[counting_col]
        .reset_index()
        .pivot_table(
            index=grouping_variables,
            columns=label_col,
            values=[counting_col],
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )

    del grouping_variables_temp
    grouping_variables_temp2 = grouping_variables.copy()
    grouping_variables_temp2.extend(["good", "fraud"])

    aggregation_fraud_ratio.columns = aggregation_fraud_ratio.columns.droplevel()
    aggregation_fraud_ratio.columns = grouping_variables_temp2
    aggregation_fraud_ratio["fraud_ratio"] = aggregation_fraud_ratio["fraud"] / (
        aggregation_fraud_ratio["good"] + aggregation_fraud_ratio["fraud"]
    )

    return aggregation_fraud_ratio


def create_payment_risk_categories():
    # Two years back, rounded back to the 1st of that month
    start_date = (datetime.today() - relativedelta(years=2)).replace(day=1).date()

    QUERY_SUBSCRIPTIONS = read_sql_file(
        "./dags/anomaly_detection/sql/subscription_payments.sql"
    ).format(start_date=start_date)

    conn = get_engine(REDSHIFT_CONN_ID)

    subscriptions_chunks = pd.read_sql(QUERY_SUBSCRIPTIONS, conn, chunksize=5000)

    subscriptions = pd.DataFrame()
    subscriptions = pd.concat(subscriptions_chunks)
    logging.info("Fetched subscriptions")

    subscriptions = subscriptions[
        (
            (
                (subscriptions["shipping_country"].isin(["Germany"]))
                & (subscriptions["shipping_pc"].str.len().isin([5]))
            )
            | (
                (subscriptions["shipping_country"].isin(["Austria"]))
                & (subscriptions["shipping_pc"].str.len().isin([4]))
            )
            | (
                (subscriptions["shipping_country"].isin(["Netherlands"]))
                & (subscriptions["shipping_pc"].str.len().isin([7]))
            )
            | (
                (subscriptions["shipping_country"].isin(["Spain"]))
                & (subscriptions["shipping_pc"].str.len().isin([5]))
            )
        )
    ].drop_duplicates()

    QUERY_LABELS = read_sql_file("./dags/anomaly_detection/sql/spectrum_labels3.sql")

    labels = pd.read_sql(QUERY_LABELS, conn)
    logging.info("Fetched labels")
    labels["customer_id"] = labels["customer_id"].astype(int)
    # Make column bool
    labels["is_blacklisted"] = labels["is_blacklisted"] == "True"
    labels["fraud_yn"] = (labels["label"].isin(["fraud"])) | (
        labels.is_blacklisted
        & (~labels["label"].isin(["good", "uncertain", "recovered good"]))
    )
    labels = labels[["customer_id", "fraud_yn"]]

    subscriptions = subscriptions.merge(labels, on="customer_id", how="left")
    del labels

    QUERY_ORDER_PAYMENT = read_sql_file(
        "./dags/anomaly_detection/sql/order_payment.sql"
    )

    order_payment_chunks = pd.read_sql(QUERY_ORDER_PAYMENT, conn, chunksize=5000)

    order_payment = pd.DataFrame()
    order_payment = pd.concat(order_payment_chunks)
    logging.info("Fetched order payment")

    order_payment = order_payment[
        order_payment["order_id"].isin(subscriptions["order_id"])
    ].copy()

    # Set empty string to None
    # This is to match the values coming from the RS table to the previous behavior of PG
    order_payment.loc[
        order_payment["credit_card_bank_name"] == "", "credit_card_bank_name"
    ] = None
    order_payment.loc[order_payment["paypal_verified"] == "", "paypal_verified"] = None

    # Set `paypal_verified` to _null_ if `paypal_verified` is NaN
    # and `payment_method` is PayPal
    order_payment.loc[
        (order_payment["payment_method"].isin(["paypal-gateway", "PayPal"]))
        & (order_payment["paypal_verified"].isna()),
        "paypal_verified",
    ] = "null"

    # Replace empty string with NA
    # Combine `card_issuer` (1st choice) & `credit_card_bank_name` (2nd choice)
    order_payment.loc[order_payment["card_issuer"] == "", "card_issuer"] = None
    order_payment.loc[
        (order_payment["card_issuer"].isna())
        & (~order_payment["credit_card_bank_name"].isna()),
        "card_issuer",
    ] = order_payment.loc[
        (order_payment["card_issuer"].isna())
        & (~order_payment["credit_card_bank_name"].isna()),
        "credit_card_bank_name",
    ]

    # Map new bank names from `BANK_MAPPING` to `card_issuer`
    order_payment["card_issuer"] = order_payment["card_issuer"].map(
        lambda x: BANK_MAPPING.get(x, x)
    )

    # Create `payment_category_penalty`
    # Add columns from 'order_payment'
    subscriptions = subscriptions.merge(
        order_payment, how="left", on=["customer_id", "order_id"], suffixes=["", "_op"]
    )
    del order_payment

    # Add prefix to 'paypal_verified'
    subscriptions["paypal_verified"] = (
        "paypal_verified_" + subscriptions["paypal_verified"]
    )

    # `payment_category_penalty`: Use `paypal_verified` or `card_issuer` if possible
    # Assign value for `payment_category_penalty` based on
    # `payment_type` & `payment_method_op` for remaining values
    subscriptions["payment_category_penalty"] = None

    subscriptions.loc[
        subscriptions["payment_type"].str.lower().isin(["paypal"]),
        "payment_category_penalty",
    ] = subscriptions.loc[
        subscriptions["payment_type"].str.lower().isin(["paypal"]), "paypal_verified"
    ]
    subscriptions.loc[
        ~subscriptions["payment_type"].str.lower().isin(["paypal"]),
        "payment_category_penalty",
    ] = subscriptions.loc[
        ~subscriptions["payment_type"].str.lower().isin(["paypal"]), "card_issuer"
    ]

    subscriptions.loc[
        subscriptions[["payment_category_penalty", "payment_type", "payment_method_op"]]
        .isnull()
        .all(axis=1),
        "payment_category_penalty",
    ] = "NA_order_not_in_order_payment_table"
    subscriptions.loc[
        subscriptions["payment_category_penalty"].isna()
        & (subscriptions["payment_type"].isin(["credit_card"]))
        & (subscriptions["payment_method_op"].isin(["AdyenContract"])),
        "payment_category_penalty",
    ] = "NA_cc_AdyenContract"
    subscriptions.loc[
        subscriptions["payment_category_penalty"].isna()
        & (subscriptions["payment_type"].isin(["sepa"]))
        & (subscriptions["payment_method_op"].isin(["sepa-gateway"])),
        "payment_category_penalty",
    ] = "NA_sepa_sepa-gateway"
    subscriptions["payment_category_penalty"] = subscriptions[
        "payment_category_penalty"
    ].fillna("NA_other")
    subscriptions["is_card_issuer"] = subscriptions["payment_category_penalty"].isin(
        subscriptions["card_issuer"]
    )

    subscriptions.drop(
        columns=["payment_method_op", "payment_type", "paypal_verified"], inplace=True
    )

    # Calculate overall (all countries) `fraud_yn` for each `payment_category_penalty`
    payment_category_penalty_all_countries_ratio = calculate_fraud_ratio(
        df=subscriptions,
        grouping_variables=["payment_category_penalty"],
        label_col="fraud_yn",
    )
    payment_category_penalty_all_countries_ratio.rename(
        columns={
            "fraud_ratio": "fraud_ratio_overall",
            "good": "good_overall",
            "fraud": "fraud_overall",
        },
        inplace=True,
    )

    # Calculate each country's `fraud_yn` for each `payment_category_penalty`
    payment_category_penalty_individual_country_ratio = calculate_fraud_ratio(
        df=subscriptions,
        grouping_variables=["shipping_country", "payment_category_penalty"],
        label_col="fraud_yn",
    )
    payment_category_penalty_individual_country_ratio.rename(
        columns={
            "fraud_ratio": "fraud_ratio_country",
            "good": "good_country",
            "fraud": "fraud_country",
        },
        inplace=True,
    )

    # Combine the overall and country-specific fraud staistics
    stats_payment_category = (
        subscriptions[
            ["shipping_country", "payment_category_penalty", "is_card_issuer"]
        ]
        .merge(
            payment_category_penalty_all_countries_ratio,
            on="payment_category_penalty",
            how="left",
        )
        .merge(
            payment_category_penalty_individual_country_ratio,
            on=["shipping_country", "payment_category_penalty"],
            how="left",
            suffixes=["_overall", "_country"],
        )
        .sort_values(["shipping_country", "payment_category_penalty"])
    )

    # Calculate median over all payments and only for card_issuer ones
    overall_median_fraud_ratio = (
        stats_payment_category.groupby("shipping_country")
        .median()
        .reset_index()[["shipping_country", "fraud_ratio_country"]]
        .rename(columns={"fraud_ratio_country": "overall_median_fraud_ratio"})
        .round(4)
    )

    card_issuer_median_fraud_ratio = (
        stats_payment_category[stats_payment_category["is_card_issuer"]]
        .groupby("shipping_country")
        .median()
        .reset_index()[["shipping_country", "fraud_ratio_country"]]
        .rename(columns={"fraud_ratio_country": "card_issuer_median_fraud_ratio"})
        .round(4)
    )

    # Add both median stats to `stats_payment_category`, remove duplicates
    stats_payment_category_all = (
        stats_payment_category.merge(
            overall_median_fraud_ratio, how="left", on="shipping_country"
        )
        .merge(card_issuer_median_fraud_ratio, how="left", on="shipping_country")
        .drop_duplicates()
    )
    del stats_payment_category

    # Use `df` as name to avoid long lines
    df = stats_payment_category_all.copy()

    # Initialize column 'fraud_ratio'
    df["fraud_ratio"] = np.nan

    # If there's more than THRESHOLD_USE_INDIVIDUAL_COUNTRY total cases for a country,
    # we use the specific country's fraud rate
    df.loc[
        (df.good_country + df.fraud_country > THRESHOLD_USE_INDIVIDUAL_COUNTRY),
        "fraud_ratio",
    ] = df.loc[
        (df.good_country + df.fraud_country > THRESHOLD_USE_INDIVIDUAL_COUNTRY),
        "fraud_ratio",
    ].fillna(
        df["fraud_ratio_country"]
    )

    # Next use overall fraud ratio
    df["fraud_ratio"] = df["fraud_ratio"].fillna(df["fraud_ratio_overall"])

    # Use 'card_issuer_median_fraud_ratio' for still missing 'card_issuer' values
    df.loc[(df["fraud_ratio"].isna()) & (df["is_card_issuer"]), "fraud_ratio"] = df.loc[
        (df["fraud_ratio"].isna()) & (df["is_card_issuer"]),
        "card_issuer_median_fraud_ratio",
    ]

    # Use 'overall_median_fraud_ratio' for remaining missing values
    df["fraud_ratio"] = df["fraud_ratio"].fillna(df["overall_median_fraud_ratio"])

    COUNTRY_CODES = {
        "Germany": "DE",
        "Austria": "AT",
        "Netherlands": "NL",
        "Spain": "ES",
    }

    df["country_code"] = df["shipping_country"].replace(COUNTRY_CODES)
    df["good_fraud"] = df[["good_country", "fraud_country"]].apply(
        lambda x: " - ".join(x.astype(str)), axis=1
    )
    df["payment_risk_info"] = (
        round(df["fraud_ratio"] * 100, 1).astype(str)
        + "%"
        + " | "
        + df["good_fraud"]
        + " | "
        + df["country_code"]
        + " | "
        + df["payment_category_penalty"]
    )

    df["payment_category_fraud_rate"] = np.nan
    df.loc[df["fraud_ratio"] > 0.5, "payment_category_fraud_rate"] = ">50%"
    df.loc[
        (df["fraud_ratio"] > 0.2) & (df["payment_category_fraud_rate"].isna()),
        "payment_category_fraud_rate",
    ] = ">20%"
    df.loc[
        (df["fraud_ratio"] > 0.1) & (df["payment_category_fraud_rate"].isna()),
        "payment_category_fraud_rate",
    ] = ">10%"
    df.loc[
        (df["fraud_ratio"] > 0.05) & (df["payment_category_fraud_rate"].isna()),
        "payment_category_fraud_rate",
    ] = ">5%"
    df.loc[
        (df["fraud_ratio"] <= 0.05) & (df["payment_category_fraud_rate"].isna()),
        "payment_category_fraud_rate",
    ] = "<5%"

    payment_risk_categories = df[
        [
            "shipping_country",
            "payment_category_penalty",
            "payment_category_fraud_rate",
            "fraud_ratio",
            "payment_risk_info",
        ]
    ].copy()
    payment_risk_categories = payment_risk_categories[
        ~payment_risk_categories.payment_category_penalty.isin(["NA"])
    ].copy()
    payment_risk_categories["fraud_ratio"] = payment_risk_categories[
        "fraud_ratio"
    ].round(3)
    payment_risk_categories.rename(
        columns={"fraud_ratio": "numeric_fraud_ratio"}, inplace=True
    )

    logging.info("Deleting {}".format(TABLE_NAME))

    DELETE_QUERY = read_sql_file(
        "./dags/anomaly_detection/sql/delete_all_table.sql"
    ).format(schema_name=SCHEMA_NAME, table_name=TABLE_NAME)

    conn.execute(DELETE_QUERY)

    logging.info("Writing {}".format(TABLE_NAME))

    payment_risk_categories.to_sql(
        method="multi",
        name=TABLE_NAME,
        con=conn,
        schema=SCHEMA_NAME,
        if_exists="append",
        index=False,
    )
