import numpy as np
import pandas as pd

from plugins.utils.df_tools import fill_na, optimize_floats, optimize_ints


def fix_total_revenue_paid(df):
    """Fix weird data, where total_amount_paid < 0"""
    df.loc[df.total_revenue_paid < 0, "default_more_8_percent"] = 0
    df.loc[df.total_revenue_paid < 0, "revenue_vs_debt"] = 0
    df.loc[df.total_revenue_paid < 0, "is_dc"] = False
    df.loc[df.total_revenue_paid < 0, "total_revenue_paid"] = 0

    return df


def process_is_all_dc_cancelled(df):
    df["is_all_dc_cancelled"] = df.apply(
        lambda x: True if x.n_cancelled_from_dc == x.n_dc_subs else False, 1
    )

    return df


def process_defaulted_amount(df):
    df["defaulted_amount"] = df.apply(
        lambda x: x.outstanding_amount
        if (pd.isna(x.defaulted_amount) & (x.is_dc) & (x.outstanding_amount > 0))
        else x.defaulted_amount,
        1,
    )

    return df


def process_default_more_8(df):
    df["default_more_8_percent"] = df.apply(
        lambda x: 1
        if x.total_outstanding_amount
        > (
            x.subscription_revenue_paid
            + x.asset_paid
            - x.subscription_revenue_chargeback
            - x.subscription_revenue_refund
        )
        * 0.08
        else 0,
        1,
    )

    return df


def process_outstanding_dc_assets(df):
    df["outstanding_dc_assets"] = df.apply(
        lambda x: 0
        if (x.outstanding_dc_amount == 0) & (x.dc_debt > 0)
        else x.outstanding_assets,
        1,
    )

    return df


def process_outstanding_assets(df):
    df["has_outstanding_assets"] = df.apply(
        lambda x: True if x.outstanding_dc_assets > 0 else False, 1
    )
    return df


def process_default_percent_recovered(df):
    df["default_percent_recovered"] = df.apply(
        lambda x: 1
        if x.outstanding_value > (x.total_revenue_paid + x.dc_paid) * 0.08
        else 0,
        1,
    )

    return df


def process_default_percentage(df):
    df["default_percent"] = df.apply(default_percentage_, 1)
    return df


def clean_data(data):
    # Drop duplicates. There shouldn't be duplicates, but there is a problem
    # for some multiple accounts in SF that must be fixed.
    data.drop_duplicates(subset=["customer_id"], inplace=True)

    data = optimize_floats(data)
    data = optimize_ints(data)

    # NA asset payment
    # Not in DC customers
    fill_0_cols = ["asset_paid", "asset_payment_due", "max_delay_for_dc", "dc_paid"]
    data[fill_0_cols] = fill_na(data, fill_0_cols, 0)

    fill_1_cols = ["outstanding_assets", "n_cancelled_from_dc"]
    data[fill_1_cols] = fill_na(data, fill_1_cols, -1)

    # Not generated payments for dc asset
    data.loc[pd.isna(data.dc_debt) & (data.is_dc.isin([True])), "dc_debt"] = 0
    data.loc[
        pd.isna(data.outstanding_dc_amount) & (data.is_dc.isin([True])),
        "outstanding_dc_amount",
    ] = 0

    # Customers never in dc filled with -1
    not_dc_cols = ["dc_debt", "outstanding_dc_amount"]
    data[not_dc_cols] = fill_na(data, not_dc_cols, -1)

    # No failed payments or no orders placed in that time
    no_orders = [
        "tot_value_cancelled_orders",
        "total_value_last_month",
        "tot_value_not_cancelled_orders",
        "n_orders_declined_last_month",
    ]
    data[no_orders] = fill_na(data, no_orders, -1)

    data[["outstanding_asset_payment"]] = data[["outstanding_asset_payment"]].fillna(0)
    data.loc[data.outstanding_asset_payment < 0, "outstanding_asset_payment"] = 0

    data["total_outstanding_amount"] = (
        data.outstanding_amount + data.outstanding_asset_payment
    )

    data["revenue_vs_debt"] = np.maximum(
        data["outstanding_dc_amount"], data["outstanding_amount"]
    ) + (
        data["subscription_revenue_chargeback"]
        + data["outstanding_asset_payment"]
        + data["subscription_revenue_refund"]
        - data["asset_paid"]
        - data["subscription_revenue_paid"]
    )

    data["total_revenue_paid"] = (
        data["subscription_revenue_paid"]
        + data["asset_paid"]
        - data["subscription_revenue_refund"]
        - data["subscription_revenue_chargeback"]
    )

    data = process_is_all_dc_cancelled(data)
    data = process_defaulted_amount(data)

    data[["defaulted_amount"]] = data[["defaulted_amount"]].fillna(0)

    # Avoid decimal precision
    data = data.round(4)

    data = process_default_more_8(data)
    # If the outstanding asset has been paid, consider it as returned
    data = process_outstanding_dc_assets(data)

    # Customers who are wrongly in DC_table, who where handed over but
    # then cancelled, are considered as is_dc = False.
    data.loc[
        (data.is_all_dc_cancelled.isin([True])) & (data.avg_dpd <= 10), "is_dc"
    ] = False
    data.loc[(data.is_dc.isin([False])), "outstanding_dc_amount"] = -1

    data = fix_total_revenue_paid(data)

    data["outstanding_value"] = (
        data.outstanding_dc_amount + data.total_outstanding_amount
    )

    data["amount_percent"] = data.total_revenue_paid / (
        data.outstanding_value + data.total_revenue_paid
    )

    data = process_outstanding_assets(data)

    data = process_default_percent_recovered(data)
    data = process_default_percentage(data)

    data["time_before_failed_"] = data["time_before_failed_"].fillna(-1)
    data.loc[data.time_before_failed_ > 12, "time_before_failed_"] = 12
    data.loc[data.time_before_failed_ < 0, "time_before_failed_"] = 12
    data["time_before_failed_"] = data["time_before_failed_"].round()

    data.loc[
        (data.failed_subscriptions == 0) & (data.is_dc.isin([False])),
        "max_delay_for_dc",
    ] = 0

    # change active status for some subscriptions that should be cancelled but they're not (bug)
    data.loc[(data.is_returned.isin([True])), "active_subscription_value"] = 0

    # customers who should have been in DC but not
    data.loc[
        (data["is_dc"].isin([False]))
        & (data["max_dpd_master"] > 60)
        & (data["time_before_failed_"] < 4)
        & (data["failed_subscriptions"] >= 3)
        & (data["amount_percent"] < 0.4)
        & (  # exclude customer who paid already some, the 'real' CD customers
            ~data["is_returned"].isin([True])
        )
        & (  # exclude customers who returned the asset
            (data["written_off_assets"] > 0) | (data["has_assets"] > 0)
        ),
        "is_dc",
    ] = True

    data.loc[
        (data.is_dc.isin([True])) & (data.outstanding_dc_assets < 0),
        "outstanding_value",
    ] = (
        data.outstanding_dc_amount_v2 + data.total_outstanding_amount
    )

    data.loc[
        (data.is_dc.isin([True])) & (data.outstanding_dc_assets < 0), "amount_percent"
    ] = data.total_revenue_paid / (data.outstanding_value + data.total_revenue_paid)

    data.loc[
        (data.is_dc.isin([True])) & (data.outstanding_dc_assets < 0),
        "outstanding_dc_assets",
    ] = 1

    data.loc[data.outstanding_dc_assets > 0, "has_outstanding_assets"] = True

    # Round up to decimal
    data = data.round(4)

    # Exclude Inf values
    data = data[data.amount_percent != np.inf]

    # Set customer_id as index
    data = data.set_index("customer_id")

    data = data[
        [
            "active_subscription_value",
            "failed_subscriptions",
            "is_dc",
            "total_subscription_value",
            "total_committed_value",
            "max_dpd_master",
            "avg_dpd",
            "avg_dpd_recent",
            "n_payments_da",
            "paid_only_first_payments",
            "n_orders_declined_last_month",
            "total_value_last_month",
            "tot_value_cancelled_orders",
            "tot_value_not_cancelled_orders",
            "defaulted_amount",
            "max_delay_for_dc",
            "total_outstanding_amount",
            "revenue_vs_debt",
            "total_revenue_paid",
            "time_before_failed_",
            "outstanding_dc_assets",
            "outstanding_value",
            "amount_percent",
            "has_outstanding_assets",
            "failed_percent",
            "default_percent",
        ]
    ]

    return data.fillna(0)


def default_percentage_(x):
    """
    Function that returns 0 for recovered cases, 1 for default and fraud, -1 for good and uncertain.
    It combines 2 attributes: default_more_8_percent, default_percent_recovered.
    """
    if (x.default_percent_recovered == 0) & (x.default_more_8_percent == 0):
        if x.is_dc:
            if x.outstanding_value <= 150:
                return 0
            else:
                return 1
        if not x.is_dc:
            return -1

    if (x.default_percent_recovered == 1) & (x.default_more_8_percent == 0):
        return 1

    if (x.default_percent_recovered == 0) & (x.default_more_8_percent == 1):
        if x.max_dpd_master > 58:
            return 1
        else:
            return -1

    if (x.default_percent_recovered == 1) & (x.default_more_8_percent == 1):
        if x.is_dc:
            if (x.outstanding_dc_amount <= 0) & (x.max_dpd_master < 58):
                return 0
            else:
                return 1
        elif x.max_dpd_master > 58:
            return 1
        else:
            return -1
