import logging

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from prophet import Prophet

from dags.cancellation_forecasting.db_queries import (query_dynamic_targets,
                                                      query_subscriptions,
                                                      query_targets)
from dags.cancellation_forecasting.utils import (aggregate_subscriptions,
                                                 create_cohort_idx,
                                                 extract_n_weekdays,
                                                 fill_na_values,
                                                 fill_past_missing_data,
                                                 include_holidays)

logger = logging.getLogger(__name__)


def create_dataframe(
    start_date,
    end_date,
    train_test_cutoff_date,
    month_cutoff,
    subcategory_data,
    use_targets,
):
    logging.info("Query subscriptions data for Germany")
    final_data = query_subscriptions(
        time_cutoff=month_cutoff,
        country_name="Germany",
        store="Grover Germany",
        customer_type="normal_customer",
    )

    logging.info("Extract the list of subcategories")
    subcategory_list = subcategory_data.subcategory_name.tolist()

    logging.info("Aggregate and clean the data (filter dates, subcategories, etc.)")
    cohort_data = aggregate_subscriptions(
        final_data, start_date, month_cutoff, subcategory_list
    )

    logging.info("Fill past data and create future dates")
    cohort_data = fill_past_missing_data(
        subcategory_list, start_date, end_date, cohort_data
    )
    cohort_data = cohort_data.sort_values(
        ["start_date", "cancellation_date", "subcategory_name"]
    )

    logging.info("Add subcategory encoding")
    cohort_data = cohort_data.merge(
        subcategory_data, on="subcategory_name", how="inner"
    )

    logging.info("Create and merge cohort id")
    cohort_map = create_cohort_idx(cohort_data)
    cohort_data = cohort_data.merge(cohort_map, on="start_date", how="left")

    # New features
    cohort_data["month_c"] = cohort_data.cancellation_date.dt.month
    cohort_data["month_s"] = cohort_data.start_date.dt.month
    cohort_data["year_c"] = cohort_data.cancellation_date.dt.year
    cohort_data["year_s"] = cohort_data.start_date.dt.year
    cohort_data["season_c"] = (cohort_data.month_c % 12 + 3) // 3
    cohort_data["season_s"] = (cohort_data.month_s % 12 + 3) // 3

    n_acquired_subscriptions, lag_1y_target = get_past_data_aggregations(
        final_data, subcategory_list, train_test_cutoff_date, use_targets
    )

    logging.info("Merge the number of subscriptions")
    cohort_data = cohort_data.merge(
        n_acquired_subscriptions,
        left_on=["start_date", "subcategory_name"],
        right_on=["date_month", "subcategory_name"],
        how="left",
    )

    logging.info("Merge 1-year-lag n_cancellations")
    cohort_data = cohort_data.merge(
        lag_1y_target[["cancellation_month", "y_lag1y", "subcategory_name"]],
        right_on=["cancellation_month", "subcategory_name"],
        left_on=["cancellation_date", "subcategory_name"],
        how="left",
    )

    logging.info(
        "Compute and merge the number of weekend and weekdays per month "
    )
    cohort_data = extract_n_weekdays(cohort_data)

    # Targets
    if use_targets:
        daily_targets, targets = query_targets(country="Germany", customer_type="B2C")
    else:
        targets = query_dynamic_targets(country="DE", customer_type="B2C")

    cohort_data = cohort_data.merge(
        targets, on=["start_date", "subcategory_name"], how="left"
    )
    logging.info("Define the last available target date")
    max_target_date = targets.start_date.max()

    cohort_data.loc[
        (cohort_data.start_date >= train_test_cutoff_date)
        & (cohort_data.start_date <= max_target_date),
        "n_cohort_subscriptions", ] = cohort_data["n_subscriptions"]

    cohort_data.loc[
        (cohort_data.start_date >= train_test_cutoff_date)
        & (cohort_data.start_date <= max_target_date),
        "cohort_asv",
    ] = cohort_data["asv"]

    if use_targets:
        n_subs_first_week_of_month_target = (
            daily_targets[daily_targets.subscription_day <= 7]
            .rename(columns={"start_date": "cancellation_date"})
            .groupby(["cancellation_date", "subcategory_name"])
            .n_subscriptions.sum()
            .rename("n_subs_first_week_of_month_target")
        )
        n_subs_last_week_of_month_target = (
            daily_targets[daily_targets.subscription_day > 23]
            .rename(columns={"start_date": "cancellation_date"})
            .groupby(["cancellation_date", "subcategory_name"])
            .n_subscriptions.sum()
            .rename("n_subs_last_week_of_month_target")
        )

        cohort_data = cohort_data.merge(
            n_subs_first_week_of_month_target,
            on=["cancellation_date", "subcategory_name"],
            how="left",
        ).merge(
            n_subs_last_week_of_month_target,
            on=["cancellation_date", "subcategory_name"],
            how="left",
        )

    logging.info("Fill null values in cohort_data")
    cohort_data = fill_na_values(cohort_data, max_target_date, train_test_cutoff_date, use_targets)

    logging.info("Import holidays")
    years = [
        year
        for year in range(
            cohort_data.start_date.min().year, cohort_data.start_date.max().year + 1
        )
    ]
    cohort_data = include_holidays(cohort_data, years, "Germany")

    logging.info("Split train and future data")
    future_data = cohort_data[(cohort_data.cancellation_date >= train_test_cutoff_date)]
    cohort_data = cohort_data[~cohort_data.index.isin(future_data.index)]

    logging.info(
        "Project future acquisitions targets for the subcategories that use an RF model "
        "that doesn't support null values"
    )
    n_subc_projections = predict_ts_targets(
        cohort_data,
        [
            "Scooters",
            "Virtual Reality",
            "Intelligent Security",
            "CamCorder",
            "e-readers",
        ],
        "n_cohort_subscriptions",
    )
    future_data = future_data.merge(
        n_subc_projections[["ds", "yhat", "subcategory_name"]].rename(
            columns={"ds": "start_date"}
        ),
        how="left",
        on=["start_date", "subcategory_name"],
    )
    future_data.loc[
        (
            future_data.subcategory_name.isin(
                [
                    "Scooters",
                    "Virtual Reality",
                    "Intelligent Security",
                    "CamCorder",
                    "e-readers",
                ]
            )
        )
        & (future_data.n_cohort_subscriptions.isna()),
        "n_cohort_subscriptions",
    ] = future_data.yhat

    if use_targets:
        future_data.loc[
            (
                future_data.subcategory_name.isin(
                    [
                        "Scooters",
                        "Virtual Reality",
                        "Intelligent Security",
                        "CamCorder",
                        "e-readers",
                    ]
                )
            )
            & (future_data.n_subs_first_week_of_month.isna()),
            "n_subs_first_week_of_month",
        ] = (
            future_data.yhat * 0.25
        )

        future_data.loc[
            (
                future_data.subcategory_name.isin(
                    [
                        "Scooters",
                        "Virtual Reality",
                        "Intelligent Security",
                        "CamCorder",
                        "e-readers",
                    ]
                )
            )
            & (future_data.n_subs_last_week_of_month.isna()),
            "n_subs_last_week_of_month",
        ] = (
            future_data.yhat * 0.25
        )

    avg_asv = (
        final_data.groupby(["subcategory_name", "date_month"])
        .subscription_value.mean()
        .dropna()
        .reset_index()
    )

    return cohort_data, future_data, avg_asv


def get_past_data_aggregations(final_data, subcategory_list, train_test_cutoff_date, use_targets):

    logging.info("Compute the number of subscriptions")
    if use_targets:
        n_acquired_subscriptions = final_data.groupby(
            ["date_month", "subcategory_name"]
        ).agg(
            n_subs_first_week_of_month=("subscription_day", lambda x: sum(x <= 7)),
            n_subs_last_week_of_month=("subscription_day", lambda x: sum(x > 23)),
            n_cohort_subscriptions=("subscription_id", "count"),
            cohort_asv=("subscription_value", 'sum'),
        )
    else:
        n_acquired_subscriptions = final_data.groupby(
            ["date_month", "subcategory_name"]
        ).agg(
            n_cohort_subscriptions=("subscription_id", "count"),
            cohort_asv=("subscription_value", 'sum'),
        )

    logging.info("Compute 1-year lag variable for the target (n_cancellations)")
    lag_1y_target = (
        final_data.groupby(["cancellation_month", "subcategory_name"])
        .size()
        .reset_index()
    )
    lag_1y_target["y_lag1y"] = lag_1y_target.groupby(["subcategory_name"])[0].shift(12)
    # Need this to add values also for the future data:
    for subcat in subcategory_list:
        for month in pd.date_range(
            train_test_cutoff_date,
            pd.to_datetime(train_test_cutoff_date) + relativedelta(months=12),
            freq="MS",
        ).tolist():
            lag_value = lag_1y_target[
                (lag_1y_target.subcategory_name == subcat)
                & (lag_1y_target.cancellation_month == month + relativedelta(years=-1))
            ][0]

            if len(lag_value) == 0:
                lag_value = 0
            else:
                lag_value = lag_value.values[0]

            lag_1y_target = lag_1y_target.append(
                pd.DataFrame(
                    {
                        "cancellation_month": [month],
                        "subcategory_name": [subcat],
                        0: [np.nan],
                        "y_lag1y": [lag_value],
                    }
                )
            )

            lag_1y_target.reset_index(drop=True, inplace=True)
    logging.info("Remove side case")
    lag_1y_target = lag_1y_target.drop_duplicates(
        ["cancellation_month", "subcategory_name", "y_lag1y"]
    )
    lag_1y_target = lag_1y_target.drop_duplicates(
        ["cancellation_month", "subcategory_name"]
    )

    return n_acquired_subscriptions, lag_1y_target


def predict_ts_targets(cohort_data, subcategory_list, target_name):
    ts_forecast = pd.DataFrame()
    for s in subcategory_list:
        X = (
            cohort_data[cohort_data.subcategory_name == s]
            .groupby(["start_date"])[[target_name]]
            .max()
            .reset_index()
            .rename(columns={"start_date": "ds", target_name: "y"})
        )
        X = X.groupby(["ds"])[["y"]].sum().reset_index()

        prophet = Prophet()
        prophet.fit(X)
        future = prophet.make_future_dataframe(periods=12, freq="MS")
        forecast = prophet.predict(future)

        ts_forecast = pd.concat(
            [
                ts_forecast,
                pd.concat(
                    [
                        forecast,
                        pd.Series([s] * (forecast.shape[0])).rename("subcategory_name"),
                    ],
                    1,
                ),
            ]
        )

    return ts_forecast
