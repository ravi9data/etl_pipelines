import logging
from datetime import datetime
from re import findall

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from plugins.utils.sql import get_engine, read_sql_file

REDSHIFT_CONN_ID = "redshift"
SCHEMA_NAME = "stg_fraud_and_credit_risk"
TABLE_NAME = "asset_risk_categories"

# Use if one sku variant + grouping has >= than this value
USE_SKU_VARIANT_THRESHOLD = 200

# Use if one sku product + grouping has >= than this value
USE_SKU_PRODUCT_THRESHOLD = 30

# Use if brand & subcategory have < than this value
USE_SUBCATEGORY_ONLY_THRESHOLD = 50


def last_match_storage_space(s, pattern: str = r"([0-9]{1,3}\s{0,1}gb)") -> str:
    """
    Function to properly & easily extract the last match
    """
    found = findall(pattern, s)
    return found[-1] if found else ""


# Fix inconsistent data problems
# - Only one `sku_product` value per `asset_name` value
# - Only one `sku_variant` value per `asset_name` value
# - Only one `brand` value per `sku_product` value
# - Only one `subcategory_name` value per `sku_product` value
# - Change respective column values in `subscriptions`
def get_max_value_item_quantity(
    df: pd.DataFrame, val1: str, val2: str, stat: str = "item_quantity"
) -> pd.DataFrame:
    """
    Select val1 with the most ordered items per val2
    """

    result_df = (
        df.groupby([val2, val1])
        .sum()[stat]
        .reset_index()
        .sort_values(by=[stat], ascending=False, kind="mergesort")
        .drop_duplicates([val2])[[val2, val1]]
    )

    return result_df


# Determine for which subcategories the age of the asset (`years_since_asset_intro`) is significant
# - Calculation for all shipping countries combined
# - If year0-to-year1 OR year1-to-year2 > 1% (absolute value) OR year0-to-year2
# larger 2% (absolute value) asset age is significant
def get_categories_age_significant(
    df: pd.DataFrame,
    category_col: str = "subcategory_name",
    age_col: str = "years_since_asset_intro",
    label_col: str = "fraud_yn",
    counting_col: str = "item_quantity",
) -> pd.DataFrame:
    """
    Select val1 with the most ordered items per val2
    """

    # `pivot_table` cannot handle NaN - fillna() is needed
    df[category_col] = df[category_col].fillna("missing_" + category_col)
    asset_age_significant = (
        df.groupby([category_col, age_col, label_col])
        .sum()[counting_col]
        .reset_index()
        .pivot_table(
            index=[category_col, age_col],
            columns=label_col,
            values=[counting_col],
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )

    asset_age_significant.columns = asset_age_significant.columns.droplevel()
    asset_age_significant.columns = [category_col, age_col, "good", "fraud"]
    asset_age_significant["fraud_ratio"] = round(
        asset_age_significant["fraud"]
        / (asset_age_significant["good"] + asset_age_significant["fraud"]),
        3,
    )

    asset_age_significant = asset_age_significant[
        [category_col, age_col, "fraud_ratio"]
    ].merge(
        asset_age_significant[[category_col, age_col, "fraud_ratio"]], on=category_col
    )

    asset_age_significant["age_difference"] = (
        asset_age_significant[age_col + "_x"] - asset_age_significant[age_col + "_y"]
    )
    asset_age_significant = asset_age_significant[
        asset_age_significant["age_difference"] < 0
    ].copy()
    asset_age_significant["fraud_ratio_difference"] = (
        asset_age_significant["fraud_ratio_x"] - asset_age_significant["fraud_ratio_y"]
    )

    subcategories_asset_age_significant = asset_age_significant[
        (
            (abs(asset_age_significant["fraud_ratio_difference"]) > 0.01)
            & (asset_age_significant["age_difference"] >= -1.5)
        )
        | (
            (abs(asset_age_significant["fraud_ratio_difference"]) > 0.02)
            & (asset_age_significant["age_difference"] <= -1.5)
        )
    ][category_col].drop_duplicates()

    return subcategories_asset_age_significant


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


def create_asset_risk_categories():
    # Two years back, rounded back to the 1st of that month
    start_date = (datetime.today() - relativedelta(years=2)).replace(day=1).date()

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

    rs_engine = get_engine(REDSHIFT_CONN_ID)

    QUERY_LABELS = read_sql_file("./dags/anomaly_detection/sql/spectrum_labels3.sql")

    labels = pd.read_sql(
        QUERY_LABELS,
        rs_engine,
        columns=["customer_id", "label", "is_blacklisted"],
    )
    logging.info("Fetched labels")
    labels["customer_id"] = labels["customer_id"].astype(int)
    # Make column bool
    labels["is_blacklisted"] = labels["is_blacklisted"] == "True"
    labels["fraud_yn"] = (labels["label"].isin(["fraud"])) | (
        labels.is_blacklisted
        & (~labels["label"].isin(["good", "uncertain", "recovered good"]))
    )

    subscriptions = subscriptions.merge(labels, on="customer_id", how="left")
    del labels

    # Convert column values and create new columns for later steps
    # - Convert values of subcategory_name, brand, asset_name & sku_product to
    # lowercase and remove leading and trailing whitespaces
    # - Create asset_introduction_date column and `years_since_asset_intro`
    #    - Possible values for years_since_asset_intro: 0, 1, 2 years
    # - For special cases (Apple smartphones & laptops, Samsung smartphones)
    #    - Create `storage_space` column (e.g. 64gb, 128gb, ..., 512gb)
    #        - add a special case where the value is 1tb and integrate it into `storage_space`
    #    - Create `core_version` column (e.g. i3, i5, i7, i9)
    subscriptions[
        ["brand", "subcategory_name", "asset_name", "sku_product"]
    ] = subscriptions[["brand", "subcategory_name", "asset_name", "sku_product"]].apply(
        lambda x: x.str.lower().str.strip()
    )

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

    subscriptions["storage_space"] = subscriptions["asset_name"].apply(
        last_match_storage_space
    )
    subscriptions["storage_space_tb"] = subscriptions["asset_name"].str.extract(
        r"([0-9]{1}\s{0,1}tb)"
    )
    subscriptions.loc[
        ~subscriptions["storage_space_tb"].isna(), "storage_space"
    ] = subscriptions.loc[~subscriptions["storage_space_tb"].isna(), "storage_space_tb"]
    subscriptions.loc[
        ~(
            (subscriptions["subcategory_name"].isin(["smartphones"]))
            & (subscriptions["brand"].isin(["apple", "samsung"]))
        ),
        "storage_space",
    ] = np.nan
    subscriptions.loc[
        subscriptions["storage_space"].isin([""]), "storage_space"
    ] = np.nan
    subscriptions["storage_space"] = subscriptions["storage_space"].replace(
        " ", "", regex=True
    )

    subscriptions["core_version"] = subscriptions["asset_name"].str.extract(
        r"(i[3|5|7|9]{1})"
    )
    subscriptions.loc[
        (subscriptions["subcategory_name"].isin(["laptops"]))
        & (subscriptions["brand"].isin(["apple"]))
        & (subscriptions["core_version"].isna()),
        "core_version",
    ] = "NA.core"
    subscriptions.loc[
        ~(
            (subscriptions["subcategory_name"].isin(["laptops"]))
            & (subscriptions["brand"].isin(["apple"]))
        ),
        "core_version",
    ] = np.nan

    subscriptions.drop(
        columns=["days_since_asset_intro", "storage_space_tb"], inplace=True
    )

    asset_name_max_sku_variant = get_max_value_item_quantity(
        subscriptions, "asset_name", "sku_variant"
    )
    brand_max_sku_product = get_max_value_item_quantity(
        subscriptions, "brand", "sku_product"
    )
    subcategory_name_max_sku_product = get_max_value_item_quantity(
        subscriptions, "subcategory_name", "sku_product"
    )

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
    subscriptions.drop(
        columns=["asset_name_old", "brand_old", "subcategory_name_old"], inplace=True
    )

    subcategories_asset_age_significant = get_categories_age_significant(
        subscriptions,
        "subcategory_name",
        "years_since_asset_intro",
        "fraud_yn",
        "item_quantity",
    )

    # - Change value of `years_since_asset_intro` to empty string for subcategories
    # where age isn't significant
    # - Combine values for `storage_space`, `core_version` in column `model_attribute`
    # (most rows will be NA)
    subscriptions["years_since_asset_intro"] = (
        subscriptions["years_since_asset_intro"].astype(int).astype(str)
    )
    subscriptions.loc[
        ~subscriptions["subcategory_name"].isin(subcategories_asset_age_significant),
        "years_since_asset_intro",
    ] = ""
    subscriptions["model_attribute"] = np.nan
    subscriptions.loc[
        ~subscriptions["storage_space"].isna(), "model_attribute"
    ] = subscriptions.loc[~subscriptions["storage_space"].isna(), "storage_space"]
    subscriptions.loc[
        (subscriptions["model_attribute"].isna())
        & (~subscriptions["core_version"].isna()),
        "model_attribute",
    ] = subscriptions.loc[
        (subscriptions["model_attribute"].isna())
        & (~subscriptions["core_version"].isna()),
        "core_version",
    ]
    subscriptions.drop(columns=["storage_space", "core_version"], inplace=True)

    # Overwrite model_attribute with the value of the SKU variant with the most ordered items.
    # NAs in 'model_attribute' will not be considered here
    sku_max_model_attribute = (
        subscriptions.groupby(["sku_variant", "sku_product", "model_attribute"])
        .sum()["item_quantity"]
        .reset_index()
        .sort_values(by=["item_quantity"], ascending=False, kind="mergesort")
        .drop_duplicates(["sku_variant", "sku_product"])[
            ["sku_variant", "sku_product", "model_attribute"]
        ]
    )

    subscriptions = subscriptions.merge(
        sku_max_model_attribute,
        on=["sku_variant", "sku_product"],
        how="left",
        suffixes=["_old", ""],
    )
    subscriptions.drop(columns=["model_attribute_old"], inplace=True)

    # Calculate asset risk types
    # - based on `sku_variant`
    # - based on `sku_product`
    # - based on `uses_brand_subcategory`
    # - based on (only) `subcategory`

    # Fill in missing values as `pivot_table` cannot handle missing values
    subscriptions["model_attribute"] = subscriptions["model_attribute"].fillna("")

    sku_variant_fraud_ratio = calculate_fraud_ratio(
        subscriptions,
        label_col="fraud_yn",
        counting_col="item_quantity",
        grouping_variables=[
            "shipping_country",
            "years_since_asset_intro",
            "model_attribute",
            "subcategory_name",
            "brand",
            "sku_product",
            "sku_variant",
        ],
    )

    sku_product_fraud_ratio = calculate_fraud_ratio(
        subscriptions,
        label_col="fraud_yn",
        counting_col="item_quantity",
        grouping_variables=[
            "shipping_country",
            "years_since_asset_intro",
            "model_attribute",
            "subcategory_name",
            "brand",
            "sku_product",
        ],
    )

    brand_subcategory_fraud_ratio = calculate_fraud_ratio(
        subscriptions,
        label_col="fraud_yn",
        counting_col="item_quantity",
        grouping_variables=[
            "shipping_country",
            "years_since_asset_intro",
            "model_attribute",
            "subcategory_name",
            "brand",
        ],
    )

    subcategory_fraud_ratio = calculate_fraud_ratio(
        subscriptions,
        label_col="fraud_yn",
        counting_col="item_quantity",
        grouping_variables=[
            "shipping_country",
            "years_since_asset_intro",
            "model_attribute",
            "subcategory_name",
        ],
    )

    sku_variant_fraud_ratio = (
        sku_variant_fraud_ratio.merge(
            sku_product_fraud_ratio,
            how="outer",
            suffixes=["", ".sku_product"],
            on=[
                "shipping_country",
                "sku_product",
                "subcategory_name",
                "brand",
                "years_since_asset_intro",
                "model_attribute",
            ],
        )
        .merge(
            brand_subcategory_fraud_ratio,
            how="outer",
            suffixes=["", ".brand_subcategory"],
            on=[
                "shipping_country",
                "subcategory_name",
                "brand",
                "years_since_asset_intro",
                "model_attribute",
            ],
        )
        .merge(
            subcategory_fraud_ratio,
            how="outer",
            suffixes=["", ".subcategory"],
            on=[
                "shipping_country",
                "subcategory_name",
                "years_since_asset_intro",
                "model_attribute",
            ],
        )
    )

    # Create `risk_category` based on full `sku_variant`
    # - If a row has more than `USE_SKU_VARIANT_THRESHOLD` labels (sum of item quantity)
    # we create the `asset_risk_category`
    # based on the full `sku_variant`
    COUNTRY_CODES = {
        "Germany": "DE",
        "Austria": "AT",
        "Netherlands": "NL",
        "Spain": "ES",
    }

    sku_variant_risk_category_uses_sku_variant = sku_variant_fraud_ratio
    sku_variant_risk_category_uses_sku_variant[
        "country_code"
    ] = sku_variant_risk_category_uses_sku_variant["shipping_country"].replace(
        COUNTRY_CODES
    )

    sku_variant_risk_category_uses_sku_variant["asset_risk_category"] = np.where(
        sku_variant_risk_category_uses_sku_variant["good"]
        + sku_variant_risk_category_uses_sku_variant["fraud"]
        >= USE_SKU_VARIANT_THRESHOLD,
        sku_variant_risk_category_uses_sku_variant[
            ["sku_variant", "country_code", "years_since_asset_intro"]
        ].apply(lambda x: "_".join(x.astype(str)), axis=1),
        np.nan,
    )

    sku_variant_risk_category_uses_sku_variant = (
        sku_variant_risk_category_uses_sku_variant[
            ~sku_variant_risk_category_uses_sku_variant["asset_risk_category"].isna()
        ].copy()
    )
    sku_variant_risk_category_uses_sku_variant["asset_risk_category"] = (
        sku_variant_risk_category_uses_sku_variant["asset_risk_category"] + "years"
    )
    sku_variant_risk_category_uses_sku_variant[
        "asset_risk_category"
    ] = sku_variant_risk_category_uses_sku_variant["asset_risk_category"].str.replace(
        "_years", "", regex=False
    )
    sku_variant_risk_category_uses_sku_variant["type"] = "uses_sku_variant"
    sku_variant_risk_category_uses_sku_variant = (
        sku_variant_risk_category_uses_sku_variant[
            [
                "shipping_country",
                "subcategory_name",
                "brand",
                "sku_variant",
                "sku_product",
                "years_since_asset_intro",
                "model_attribute",
                "asset_risk_category",
                "good",
                "fraud",
                "fraud_ratio",
                "type",
            ]
        ]
    )

    # Create `risk_category` based on `sku_product`
    # - If a row from `sku_variant_ratio` wasn't already used in the previous step
    # (i.e. less than `USE_SKU_VARIANT_THRESHOLD` labels)
    # and has more than `USE_SKU_PRODUCT_THRESHOLD` labels (`True.sku_product + False.sku_product`),
    # we create the `asset_risk_category` based on the `sku_product`
    sku_variant_risk_category_uses_sku_product = sku_variant_fraud_ratio
    sku_variant_risk_category_uses_sku_product[
        "country_code"
    ] = sku_variant_risk_category_uses_sku_product["shipping_country"].replace(
        COUNTRY_CODES
    )

    sku_variant_risk_category_uses_sku_product = (
        sku_variant_risk_category_uses_sku_product[
            ~sku_variant_risk_category_uses_sku_product.index.isin(
                sku_variant_risk_category_uses_sku_variant.index
            )
        ].copy()
    )

    sku_variant_risk_category_uses_sku_product["asset_risk_category"] = np.where(
        sku_variant_risk_category_uses_sku_product["good.sku_product"]
        + sku_variant_risk_category_uses_sku_product["fraud.sku_product"]
        >= USE_SKU_PRODUCT_THRESHOLD,
        sku_variant_risk_category_uses_sku_product[
            ["sku_product", "country_code", "years_since_asset_intro"]
        ].apply(lambda x: "_".join(x.astype(str)), axis=1),
        np.nan,
    )

    sku_variant_risk_category_uses_sku_product = (
        sku_variant_risk_category_uses_sku_product[
            ~sku_variant_risk_category_uses_sku_product["asset_risk_category"].isna()
        ].copy()
    )
    sku_variant_risk_category_uses_sku_product["asset_risk_category"] = (
        sku_variant_risk_category_uses_sku_product["asset_risk_category"] + "years"
    )
    sku_variant_risk_category_uses_sku_product[
        "asset_risk_category"
    ] = sku_variant_risk_category_uses_sku_product["asset_risk_category"].str.replace(
        "_years", "", regex=False
    )
    sku_variant_risk_category_uses_sku_product["type"] = "uses_sku_product"
    sku_variant_risk_category_uses_sku_product = (
        sku_variant_risk_category_uses_sku_product[
            [
                "shipping_country",
                "subcategory_name",
                "brand",
                "sku_variant",
                "sku_product",
                "years_since_asset_intro",
                "model_attribute",
                "asset_risk_category",
                "good.sku_product",
                "fraud.sku_product",
                "fraud_ratio.sku_product",
                "type",
            ]
        ]
    )
    sku_variant_risk_category_uses_sku_product.rename(
        columns={
            "good.sku_product": "good",
            "fraud.sku_product": "fraud",
            "fraud_ratio.sku_product": "fraud_ratio",
        },
        inplace=True,
    )

    # Create remaining `risk_category`
    # - We create the remaining `risk_category` which didn't fit the critera of the last two steps
    uses_subcategory_name_and_brand_name = sku_variant_fraud_ratio
    uses_subcategory_name_and_brand_name[
        "country_code"
    ] = uses_subcategory_name_and_brand_name["shipping_country"].replace(COUNTRY_CODES)

    uses_subcategory_name_and_brand_name = uses_subcategory_name_and_brand_name[
        ~uses_subcategory_name_and_brand_name.index.isin(
            sku_variant_risk_category_uses_sku_variant.index
        )
    ].copy()
    uses_subcategory_name_and_brand_name = uses_subcategory_name_and_brand_name[
        ~uses_subcategory_name_and_brand_name.index.isin(
            sku_variant_risk_category_uses_sku_product.index
        )
    ].copy()

    uses_subcategory_name_and_brand_name[
        "asset_risk_category"
    ] = uses_subcategory_name_and_brand_name[
        [
            "brand",
            "subcategory_name",
            "model_attribute",
            "country_code",
            "years_since_asset_intro",
        ]
    ].apply(
        lambda x: "_".join(x.astype(str)), axis=1
    )

    uses_subcategory_name_and_brand_name["special_cases"] = (
        uses_subcategory_name_and_brand_name[["brand", "subcategory_name"]]
        .apply(lambda x: "_".join(x.astype(str)), axis=1)
        .isin(["apple_smartphones", "samsung_smartphones", "apple_laptops"])
    )
    uses_subcategory_name_and_brand_name.loc[
        (
            uses_subcategory_name_and_brand_name["good.brand_subcategory"]
            + uses_subcategory_name_and_brand_name["fraud.brand_subcategory"]
            < USE_SUBCATEGORY_ONLY_THRESHOLD
        )
        & (~uses_subcategory_name_and_brand_name["special_cases"]),
        "asset_risk_category",
    ] = uses_subcategory_name_and_brand_name.loc[
        (
            uses_subcategory_name_and_brand_name["good.brand_subcategory"]
            + uses_subcategory_name_and_brand_name["fraud.brand_subcategory"]
            < USE_SUBCATEGORY_ONLY_THRESHOLD
        )
        & (~uses_subcategory_name_and_brand_name["special_cases"]),
        ["subcategory_name", "country_code", "years_since_asset_intro"],
    ].apply(
        lambda x: "_".join(x.astype(str)), axis=1
    )

    uses_subcategory_name_and_brand_name = uses_subcategory_name_and_brand_name[
        ~uses_subcategory_name_and_brand_name["asset_risk_category"].isna()
    ].copy()
    uses_subcategory_name_and_brand_name["asset_risk_category"] = (
        uses_subcategory_name_and_brand_name["asset_risk_category"] + "years"
    )
    uses_subcategory_name_and_brand_name[
        "asset_risk_category"
    ] = uses_subcategory_name_and_brand_name["asset_risk_category"].str.replace(
        "_years", "", regex=False
    )
    uses_subcategory_name_and_brand_name[
        "asset_risk_category"
    ] = uses_subcategory_name_and_brand_name["asset_risk_category"].str.replace(
        "__", "_", regex=False
    )

    uses_subcategory_name_and_brand_name["type"] = "uses_subcategory"
    uses_subcategory_name_and_brand_name.loc[
        (uses_subcategory_name_and_brand_name["special_cases"])
        | (
            uses_subcategory_name_and_brand_name["good.brand_subcategory"]
            + uses_subcategory_name_and_brand_name["fraud.brand_subcategory"]
            >= USE_SUBCATEGORY_ONLY_THRESHOLD
        ),
        "type",
    ] = "uses_brand_subcategory"
    # Use correct column for 'good' / 'fraud' - based on 'type'
    uses_subcategory_name_and_brand_name.loc[
        uses_subcategory_name_and_brand_name["type"].isin(["uses_brand_subcategory"]),
        ["good", "fraud", "fraud_ratio"],
    ] = uses_subcategory_name_and_brand_name.loc[
        uses_subcategory_name_and_brand_name["type"].isin(["uses_brand_subcategory"]),
        [
            "good.brand_subcategory",
            "fraud.brand_subcategory",
            "fraud_ratio.brand_subcategory",
        ],
    ].values
    uses_subcategory_name_and_brand_name.loc[
        uses_subcategory_name_and_brand_name["type"].isin(["uses_subcategory"]),
        ["good", "fraud", "fraud_ratio"],
    ] = uses_subcategory_name_and_brand_name.loc[
        uses_subcategory_name_and_brand_name["type"].isin(["uses_subcategory"]),
        ["good.subcategory", "fraud.subcategory", "fraud_ratio.subcategory"],
    ].values
    uses_subcategory_name_and_brand_name = uses_subcategory_name_and_brand_name[
        [
            "shipping_country",
            "subcategory_name",
            "brand",
            "sku_variant",
            "sku_product",
            "years_since_asset_intro",
            "model_attribute",
            "asset_risk_category",
            "good",
            "fraud",
            "fraud_ratio",
            "type",
        ]
    ]

    # Combine the three different data frames to create `sku_variant_risk_category`
    sku_variant_risk_category = sku_variant_risk_category_uses_sku_variant.append(
        sku_variant_risk_category_uses_sku_product
    ).append(uses_subcategory_name_and_brand_name)

    # Create `sku_variant_risk_info`
    sku_variant_risk_info = sku_variant_risk_category
    sku_variant_risk_info["country_code"] = sku_variant_risk_info[
        "shipping_country"
    ].replace(COUNTRY_CODES)
    sku_variant_risk_info["fraud_ratio"] = sku_variant_risk_info["fraud_ratio"].fillna(
        0
    )
    sku_variant_risk_info["asset_risk_fraud_rate"] = np.nan
    sku_variant_risk_info.loc[
        sku_variant_risk_info["fraud_ratio"] > 0.5, "asset_risk_fraud_rate"
    ] = ">50%"
    sku_variant_risk_info.loc[
        (sku_variant_risk_info["fraud_ratio"] > 0.2)
        & (sku_variant_risk_info["asset_risk_fraud_rate"].isna()),
        "asset_risk_fraud_rate",
    ] = ">20%"
    sku_variant_risk_info.loc[
        (sku_variant_risk_info["fraud_ratio"] > 0.1)
        & (sku_variant_risk_info["asset_risk_fraud_rate"].isna()),
        "asset_risk_fraud_rate",
    ] = ">10%"
    sku_variant_risk_info.loc[
        (sku_variant_risk_info["fraud_ratio"] > 0.05)
        & (sku_variant_risk_info["asset_risk_fraud_rate"].isna()),
        "asset_risk_fraud_rate",
    ] = ">5%"
    sku_variant_risk_info.loc[
        (sku_variant_risk_info["fraud_ratio"] <= 0.01)
        & (sku_variant_risk_info["asset_risk_fraud_rate"].isna()),
        "asset_risk_fraud_rate",
    ] = "<1%"
    sku_variant_risk_info.loc[
        (sku_variant_risk_info["fraud_ratio"] <= 0.025)
        & (sku_variant_risk_info["asset_risk_fraud_rate"].isna()),
        "asset_risk_fraud_rate",
    ] = ">1%"
    sku_variant_risk_info.loc[
        (sku_variant_risk_info["fraud_ratio"] > 0.025)
        & (sku_variant_risk_info["asset_risk_fraud_rate"].isna()),
        "asset_risk_fraud_rate",
    ] = ">2.5%"

    sku_variant_risk_info[["good", "fraud"]] = (
        sku_variant_risk_info[["good", "fraud"]].round(0).astype(int)
    )
    sku_variant_risk_info["good_fraud"] = sku_variant_risk_info[
        ["good", "fraud"]
    ].apply(lambda x: " - ".join(x.astype(str)), axis=1)

    sku_variant_risk_info["description"] = np.where(
        sku_variant_risk_info["type"].isin(["uses_sku_variant"]),
        "sku_variant_specific"
        + "_"
        + sku_variant_risk_info["country_code"]
        + "_"
        + sku_variant_risk_info["years_since_asset_intro"]
        + "years",
        sku_variant_risk_info["asset_risk_category"],
    )
    sku_variant_risk_info["description"] = np.where(
        sku_variant_risk_info["type"].isin(["uses_sku_product"]),
        "sku_product_specific"
        + "_"
        + sku_variant_risk_info["country_code"]
        + "_"
        + sku_variant_risk_info["years_since_asset_intro"]
        + "years",
        sku_variant_risk_info["description"],
    )
    sku_variant_risk_info["description"] = sku_variant_risk_info[
        "description"
    ].str.replace("_years", "", regex=False)
    sku_variant_risk_info["description"] = sku_variant_risk_info[
        "description"
    ].str.replace("_$", "", regex=True)

    sku_variant_risk_info["asset_risk_info"] = (
        round(sku_variant_risk_info["fraud_ratio"] * 100, 1).astype(str)
        + "%"
        + " | "
        + sku_variant_risk_info["good_fraud"]
        + " | "
        + sku_variant_risk_info["description"]
    )

    # Merge `subscriptions` with `sku_variant_risk_info` to ensure no value is lost
    asset_risk_categories = subscriptions.merge(
        sku_variant_risk_info,
        how="left",
        on=[
            "shipping_country",
            "subcategory_name",
            "brand",
            "sku_product",
            "sku_variant",
            "years_since_asset_intro",
            "model_attribute",
        ],
    )[
        [
            "shipping_country",
            "country_code",
            "asset_name",
            "sku_variant",
            "sku_product",
            "asset_introduction_date",
            "years_since_asset_intro",
            "model_attribute",
            "brand",
            "subcategory_name",
            "fraud_ratio",
            "asset_risk_fraud_rate",
            "asset_risk_info",
            "type",
        ]
    ].drop_duplicates()

    asset_risk_categories["fraud_ratio"] = asset_risk_categories["fraud_ratio"].round(3)

    # Determine for each combination of # country_code, sku_variant, sku_product
    # the "newest" risk category and add the column 'newest_risk_category' (TRUE / FALSE)
    # that contains that information
    asset_risk_categories.loc[
        asset_risk_categories["years_since_asset_intro"] == "",
        "years_since_asset_intro",
    ] = -9
    asset_risk_categories["years_since_asset_intro"] = asset_risk_categories[
        "years_since_asset_intro"
    ].astype(int)
    asset_risk_categories["sku_product"] = asset_risk_categories[
        "sku_product"
    ].str.upper()

    max_ages = (
        asset_risk_categories.groupby(["country_code", "sku_variant", "sku_product"])[
            "years_since_asset_intro"
        ]
        .max()
        .reset_index()
    )
    max_ages.rename(
        columns={"years_since_asset_intro": "max_years_since_asset_intro"}, inplace=True
    )

    asset_risk_categories = asset_risk_categories.merge(
        max_ages, how="left", on=["country_code", "sku_variant", "sku_product"]
    )
    del max_ages

    asset_risk_categories["newest_risk_category"] = (
        asset_risk_categories["years_since_asset_intro"]
        == asset_risk_categories["max_years_since_asset_intro"]
    )
    asset_risk_categories.drop(
        columns=["shipping_country", "max_years_since_asset_intro"], inplace=True
    )
    asset_risk_categories.rename(
        columns={"model_attribute": "special_attribute"}, inplace=True
    )

    asset_risk_categories = asset_risk_categories[
        [
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
            "fraud_ratio",
            "asset_risk_fraud_rate",
            "asset_risk_info",
        ]
    ]

    # Fill missing values since this leads to problems when writing to the DWH
    asset_risk_categories["special_attribute"] = asset_risk_categories[
        "special_attribute"
    ].fillna("")
    asset_risk_categories["subcategory_name"] = asset_risk_categories[
        "subcategory_name"
    ].fillna("missing_subcategory_name")

    rs_engine = get_engine(REDSHIFT_CONN_ID)

    DELETE_QUERY = read_sql_file(
        "./dags/anomaly_detection/sql/delete_all_table.sql"
    ).format(
        schema_name=SCHEMA_NAME,
        table_name=TABLE_NAME,
    )
    rs_engine.execute(DELETE_QUERY)
    logging.info(f"Deleted content of {TABLE_NAME}.")

    asset_risk_categories["created_at"] = datetime.today()
    asset_risk_categories = asset_risk_categories.rename(
        columns={"fraud_ratio": "numeric_fraud_rate"}
    )

    asset_risk_categories.to_sql(
        method="multi",
        name=TABLE_NAME,
        con=rs_engine,
        schema=SCHEMA_NAME,
        if_exists="append",
        index=True,
        index_label="id",
    )
