import logging
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from dags.anomaly_detection.constants import BANK_MAPPING
from plugins.utils.sql import get_engine, read_sql_file

REDSHIFT_CONN_ID = "redshift"
SCHEMA_NAME = "stg_fraud_and_credit_risk"
TABLE_NAME = "payment_risk_categories_order"


def create_payment_risk_orders():

    # Ten days back
    start_date = (datetime.today() - relativedelta(days=10)).date()

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

    # Set `paypal_verified` to _null_ if `paypal_verified` is NaN and `payment_method` is PayPal
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
    # free some memory
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

    subscriptions = (
        subscriptions[
            ["customer_id", "order_id", "shipping_country", "payment_category_penalty"]
        ]
        .drop_duplicates()
        .copy()
    )

    # Import payment risk categories
    QUERY_PAYMENT_RISK_CATEGORIES = read_sql_file(
        "./dags/anomaly_detection/sql/payment_risk_categories.sql"
    )

    payment_risk_categories_chunks = pd.read_sql(
        QUERY_PAYMENT_RISK_CATEGORIES, conn, chunksize=5000
    )

    payment_risk_categories = pd.DataFrame()
    payment_risk_categories = pd.concat(payment_risk_categories_chunks)
    logging.info("Fetched payment risk categories")

    # We set 'payment_category_penalty' to 'NA_other' for categories not
    # in the 'payment_risk_categories' DF
    subscriptions.loc[
        (
            ~subscriptions["payment_category_penalty"].isin(
                payment_risk_categories["payment_category_penalty"]
            )
        ),
        "payment_category_penalty",
    ] = "NA_other"

    payment_risk_orders = subscriptions.merge(
        payment_risk_categories,
        on=["shipping_country", "payment_category_penalty"],
        how="left",
    )

    # free some memory
    del subscriptions

    # Fill missing values with values which won't be NULL in an DB table
    payment_risk_orders.loc[
        payment_risk_orders["numeric_fraud_ratio"].isna(),
        ["payment_category_fraud_rate", "numeric_fraud_ratio", "payment_risk_info"],
    ] = ["missing", -9, "missing"]
    payment_risk_orders["numeric_fraud_ratio"] = payment_risk_orders[
        "numeric_fraud_ratio"
    ].astype(float).round(3)

    payment_risk_orders = (
        payment_risk_orders[
            [
                "order_id",
                "customer_id",
                "payment_category_fraud_rate",
                "numeric_fraud_ratio",
                "payment_risk_info",
            ]
        ]
        .drop_duplicates()
        .copy()
    )

    logging.info("Deleting {}".format(TABLE_NAME))

    DELETE_QUERY = read_sql_file(
        "./dags/anomaly_detection/sql/delete_all_table.sql"
    ).format(schema_name=SCHEMA_NAME, table_name=TABLE_NAME)

    conn.execute(DELETE_QUERY)

    logging.info("Writing {}".format(TABLE_NAME))

    conn = get_engine(REDSHIFT_CONN_ID)

    logging.info(payment_risk_orders.shape)
    payment_risk_orders.to_sql(
        method="multi",
        name=TABLE_NAME,
        con=conn,
        schema=SCHEMA_NAME,
        if_exists="append",
        index=False,
        chunksize=2000
    )
