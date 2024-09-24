import logging
from datetime import datetime

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from plugins.utils.df_tools import (optimize_floats, optimize_ints,
                                    optimize_objects)
from plugins.utils.memory import clean_from_memory
from plugins.utils.sql import get_engine, read_sql_file

REDSHIFT_CONN_ID = "redshift"
SCHEMA_NAME = "stg_fraud_and_credit_risk"
TABLE_NAME = "asset_risk_categories_order"


def create_asset_risk_categories_order():
    # Ten days back
    start_date = (datetime.today() - relativedelta(days=10)).date()

    QUERY_SUBSCRIPTIONS = read_sql_file(
        "./dags/anomaly_detection/sql/subscription_assets.sql"
    ).format(start_date=start_date)

    rs_engine = get_engine(REDSHIFT_CONN_ID)

    subscriptions_chunks = pd.read_sql(QUERY_SUBSCRIPTIONS, rs_engine, chunksize=5000)

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

    # Convert column values and create new columns for later steps
    # - Convert values of subcategory_name, brand & asset_name to lowercase
    # and remove leading and trailing whitespaces
    # - Create asset_introduction_date column and `years_since_asset_intro`
    #    - Possible values for years_since_asset_intro: 0, 1, 2 years
    subscriptions[["brand", "subcategory_name", "asset_name"]] = subscriptions[
        ["brand", "subcategory_name", "asset_name"]
    ].apply(lambda x: x.str.lower().str.strip())

    subscriptions["days_since_asset_intro"] = (
        subscriptions["order_submission"].dt.date
        - subscriptions["asset_introduction_date"].dt.date
    )

    # Everything older than 2 years leads to small sample sizes in countries other than Germany
    subscriptions["years_since_asset_intro"] = np.nan
    subscriptions.loc[
        subscriptions["days_since_asset_intro"] <= pd.Timedelta(days=365),
        "years_since_asset_intro",
    ] = 0
    subscriptions.loc[
        subscriptions["days_since_asset_intro"] > pd.Timedelta(days=365),
        "years_since_asset_intro",
    ] = 1
    subscriptions.loc[
        subscriptions["days_since_asset_intro"] > pd.Timedelta(days=(365 * 2)),
        "years_since_asset_intro",
    ] = 2
    subscriptions.drop(columns=["days_since_asset_intro"], inplace=True)

    logging.info("asset_risk_categories")

    QUERY_RISK_CATEGORIES = read_sql_file(
        "./dags/anomaly_detection/sql/asset_risk_categories.sql"
    ).format(start_date=start_date)

    rs_engine = get_engine(REDSHIFT_CONN_ID)

    risk_categories_chunks = pd.read_sql(
        QUERY_RISK_CATEGORIES, rs_engine, chunksize=5000
    )

    asset_risk_categories = pd.DataFrame()
    asset_risk_categories = pd.concat(risk_categories_chunks)

    asset_risk_categories["special_attribute"] = asset_risk_categories[
        "special_attribute"
    ].fillna("")

    asset_name_max_sku_variant = asset_risk_categories[
        ["asset_name", "sku_variant"]
    ].drop_duplicates()
    brand_max_sku_product = asset_risk_categories[
        ["brand", "sku_product"]
    ].drop_duplicates()
    subcategory_name_max_sku_product = asset_risk_categories[
        ["subcategory_name", "sku_product"]
    ].drop_duplicates()
    sku_max_model_attribute = asset_risk_categories[
        ["sku_variant", "sku_product", "special_attribute"]
    ].drop_duplicates()

    subcategories_asset_age_significant = asset_risk_categories[
        ~asset_risk_categories["years_since_asset_intro"].isin([-9])
    ]["subcategory_name"].drop_duplicates()

    subscriptions = subscriptions.merge(
        asset_name_max_sku_variant, on="sku_variant", how="left", suffixes=["_old", ""]
    )
    subscriptions = subscriptions.merge(
        brand_max_sku_product, on="sku_product", how="left", suffixes=["_old", ""]
    )
    subscriptions = subscriptions.merge(
        subcategory_name_max_sku_product,
        on="sku_product",
        how="left",
        suffixes=["_old", ""],
    )
    subscriptions = subscriptions.merge(
        sku_max_model_attribute,
        on=["sku_variant", "sku_product"],
        how="left",
        suffixes=["_old", ""],
    )

    subscriptions["asset_name"] = subscriptions["asset_name"].fillna(
        subscriptions["asset_name_old"]
    )
    subscriptions["brand"] = subscriptions["brand"].fillna(subscriptions["brand_old"])
    subscriptions["subcategory_name"] = subscriptions["subcategory_name"].fillna(
        subscriptions["subcategory_name_old"]
    )
    subscriptions["special_attribute"] = subscriptions["special_attribute"].fillna("")

    subscriptions.drop(
        columns=["asset_name_old", "brand_old", "subcategory_name_old"], inplace=True
    )

    country_codes = {
        "Germany": "DE",
        "Austria": "AT",
        "Netherlands": "NL",
        "Spain": "ES",
    }
    subscriptions["country_code"] = subscriptions["shipping_country"].replace(
        country_codes
    )

    # - Change value of `years_since_asset_intro` to empty string for
    # subcategories where age isn't significant
    # - Combine values for `storage_space`, `core_version` in column
    # `model_attribute` (most rows will be NA)
    subscriptions["years_since_asset_intro"] = subscriptions[
        "years_since_asset_intro"
    ].astype(int)
    subscriptions.loc[
        ~subscriptions["subcategory_name"].isin(subcategories_asset_age_significant),
        "years_since_asset_intro",
    ] = -9

    asset_risk_categories_order = subscriptions.merge(
        asset_risk_categories[
            [
                "country_code",
                "subcategory_name",
                "brand",
                "sku_product",
                "sku_variant",
                "years_since_asset_intro",
                "newest_risk_category",
                "type",
                "special_attribute",
                "numeric_fraud_rate",
                "asset_risk_fraud_rate",
                "asset_risk_info",
            ]
        ],
        how="left",
        on=[
            "country_code",
            "subcategory_name",
            "brand",
            "sku_product",
            "sku_variant",
            "years_since_asset_intro",
            "special_attribute",
        ],
    )

    # free some memory
    clean_from_memory(subscriptions)

    asset_risk_categories_order = asset_risk_categories_order[
        [
            "subscription_id",
            "order_id",
            "customer_id",
            "newest_risk_category",
            "country_code",
            "type",
            "subcategory_name",
            "brand",
            "sku_product",
            "sku_variant",
            "asset_name",
            "special_attribute",
            "asset_introduction_date",
            "years_since_asset_intro",
            "numeric_fraud_rate",
            "asset_risk_fraud_rate",
            "asset_risk_info",
        ]
    ].drop_duplicates()

    asset_risk_categories = optimize_ints(asset_risk_categories)
    asset_risk_categories = optimize_floats(asset_risk_categories)
    asset_risk_categories = optimize_objects(asset_risk_categories, ["created_at"])

    asset_risk_categories_order_new = pd.DataFrame()
    # Fill missing values (SKU variant / product not in 'asset_risk_categories')
    # with the brand_subcategory or subcategory values
    if asset_risk_categories_order.isnull().values.any():
        asset_risk_categories_order_new = asset_risk_categories_order[
            asset_risk_categories_order.isnull().any(axis=1)
        ].merge(
            asset_risk_categories.loc[
                (asset_risk_categories["type"].isin(["uses_brand_subcategory"])),
                [
                    "country_code",
                    "subcategory_name",
                    "brand",
                    "years_since_asset_intro",
                    "special_attribute",
                    "numeric_fraud_rate",
                    "asset_risk_fraud_rate",
                    "asset_risk_info",
                ],
            ].drop_duplicates(),
            how="left",
            on=[
                "country_code",
                "subcategory_name",
                "brand",
                "years_since_asset_intro",
                "special_attribute",
            ],
            suffixes=["", "_brand_subcategory"],
        )

        asset_risk_categories_order_new = asset_risk_categories_order_new.merge(
            asset_risk_categories.loc[
                (asset_risk_categories["type"].isin(["uses_subcategory"])),
                [
                    "country_code",
                    "subcategory_name",
                    "years_since_asset_intro",
                    "special_attribute",
                    "numeric_fraud_rate",
                    "asset_risk_fraud_rate",
                    "asset_risk_info",
                ],
            ].drop_duplicates(),
            how="left",
            on=[
                "country_code",
                "subcategory_name",
                "years_since_asset_intro",
                "special_attribute",
            ],
            suffixes=["", "_subcategory"],
        )

    # free some memory
    clean_from_memory(asset_risk_categories)

    if not asset_risk_categories_order_new.empty:
        asset_risk_categories_order_new["type"] = "uses_brand_subcategory"
        asset_risk_categories_order_new["newest_risk_category"] = True

        asset_risk_categories_order_new[
            "numeric_fraud_rate"
        ] = asset_risk_categories_order_new["numeric_fraud_rate"].fillna(
            asset_risk_categories_order_new["numeric_fraud_rate_brand_subcategory"]
        )
        asset_risk_categories_order_new[
            "asset_risk_fraud_rate"
        ] = asset_risk_categories_order_new["asset_risk_fraud_rate"].fillna(
            asset_risk_categories_order_new["asset_risk_fraud_rate_brand_subcategory"]
        )
        asset_risk_categories_order_new[
            "asset_risk_info"
        ] = asset_risk_categories_order_new["asset_risk_info"].fillna(
            asset_risk_categories_order_new["asset_risk_info_brand_subcategory"]
        )

        asset_risk_categories_order_new.drop(
            columns=[
                "numeric_fraud_rate_brand_subcategory",
                "asset_risk_fraud_rate_brand_subcategory",
                "asset_risk_info_brand_subcategory",
            ],
            inplace=True,
        )

        #    if 'numeric_fraud_rate_subcategory' in asset_risk_categories_order_new:
        asset_risk_categories_order_new[
            "numeric_fraud_rate"
        ] = asset_risk_categories_order_new["numeric_fraud_rate"].fillna(
            asset_risk_categories_order_new["numeric_fraud_rate_subcategory"]
        )
        asset_risk_categories_order_new[
            "asset_risk_fraud_rate"
        ] = asset_risk_categories_order_new["asset_risk_fraud_rate"].fillna(
            asset_risk_categories_order_new["asset_risk_fraud_rate_subcategory"]
        )
        asset_risk_categories_order_new[
            "asset_risk_info"
        ] = asset_risk_categories_order_new["asset_risk_info"].fillna(
            asset_risk_categories_order_new["asset_risk_info_subcategory"]
        )

        asset_risk_categories_order_new["type"] = np.where(
            asset_risk_categories_order_new["numeric_fraud_rate"]
            == asset_risk_categories_order_new["numeric_fraud_rate_subcategory"],
            "uses_subcategory",
            asset_risk_categories_order_new["type"],
        )

        asset_risk_categories_order_new.drop(
            columns=[
                "numeric_fraud_rate_subcategory",
                "asset_risk_fraud_rate_subcategory",
                "asset_risk_info_subcategory",
            ],
            inplace=True,
        )

        asset_risk_categories_order = asset_risk_categories_order.merge(
            asset_risk_categories_order_new[
                [
                    "subscription_id",
                    "type",
                    "newest_risk_category",
                    "numeric_fraud_rate",
                    "asset_risk_fraud_rate",
                    "asset_risk_info",
                ]
            ],
            how="left",
            on="subscription_id",
            suffixes=["", "_new"],
        )

        asset_risk_categories_order["numeric_fraud_rate"] = asset_risk_categories_order[
            "numeric_fraud_rate"
        ].fillna(asset_risk_categories_order["numeric_fraud_rate_new"])
        asset_risk_categories_order[
            "asset_risk_fraud_rate"
        ] = asset_risk_categories_order["asset_risk_fraud_rate"].fillna(
            asset_risk_categories_order["asset_risk_fraud_rate_new"]
        )
        asset_risk_categories_order["asset_risk_info"] = asset_risk_categories_order[
            "asset_risk_info"
        ].fillna(asset_risk_categories_order["asset_risk_info_new"])
        asset_risk_categories_order["type"] = asset_risk_categories_order[
            "type"
        ].fillna(asset_risk_categories_order["type_new"])
        asset_risk_categories_order[
            "newest_risk_category"
        ] = asset_risk_categories_order["newest_risk_category"].fillna(
            asset_risk_categories_order["newest_risk_category_new"]
        )

        asset_risk_categories_order.drop(
            columns=[
                "numeric_fraud_rate_new",
                "asset_risk_fraud_rate_new",
                "asset_risk_info_new",
                "type_new",
                "newest_risk_category_new",
            ],
            inplace=True,
        )

    rs_engine = get_engine(REDSHIFT_CONN_ID)

    DELETE_QUERY = read_sql_file(
        "./dags/anomaly_detection/sql/delete_all_table.sql"
    ).format(
        schema_name=SCHEMA_NAME,
        table_name=TABLE_NAME,
    )
    rs_engine.execute(DELETE_QUERY)
    logging.info(f"Deleted content of {TABLE_NAME}.")

    asset_risk_categories_order["created_at"] = datetime.today()

    asset_risk_categories_order = asset_risk_categories_order.filter(
        items=[
            "subscription_id",
            "order_id",
            "created_at",
            "customer_id",
            "asset_risk_fraud_rate",
            "asset_risk_info",
            "numeric_fraud_rate",
        ]
    )

    asset_risk_categories_order = optimize_ints(asset_risk_categories_order)
    asset_risk_categories_order = optimize_floats(asset_risk_categories_order)

    #  asset_risk_categories_order = asset_risk_categories_order.head(50000)
    #  logging.info(asset_risk_categories_order.shape)

    asset_risk_categories_order.to_sql(
        method="multi",
        name=TABLE_NAME,
        con=rs_engine,
        schema=SCHEMA_NAME,
        chunksize=5000,
        if_exists="append",
        index=False,
    )
