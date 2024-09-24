import logging

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

import dags.cancellation_forecasting.austria.constants as const
from dags.cancellation_forecasting.db_queries import (query_dynamic_targets,
                                                      query_subscriptions,
                                                      query_targets)
from dags.cancellation_forecasting.germany.create_data import \
    predict_ts_targets
from dags.cancellation_forecasting.germany_B2B.create_data import \
    get_past_data_aggregations
from dags.cancellation_forecasting.utils import (aggregate_subscriptions,
                                                 compute_lag_variables,
                                                 create_cohort_idx,
                                                 fill_past_missing_data,
                                                 filter_old_dates,
                                                 include_holidays,
                                                 predict_ts_target)

logger = logging.getLogger(__name__)


def create_dataframe(
        start_date,
        end_date,
        train_test_cutoff_date,
        month_cutoff,
        subcategory_data,
        use_targets
):

    logging.info('Query the subscriptions data')
    final_data = query_subscriptions(
        time_cutoff=month_cutoff,
        country_name=const.COUNTRY,
        store='Grover International',
        customer_type='normal_customer'
    )

    logging.info('Extract the list of subcategories')
    subcategory_list = subcategory_data.subcategory_name.tolist()

    logging.info('Aggregate and clean the data (filter dates, subcategories, etc.)')
    cohort_data = aggregate_subscriptions(final_data, start_date, month_cutoff, subcategory_list)

    logging.info('Fill past data and create future dates')
    cohort_data = fill_past_missing_data(subcategory_list, start_date, end_date, cohort_data)
    cohort_data = cohort_data.sort_values(['start_date', 'cancellation_date', 'subcategory_name'])

    logging.info('Exclude data for subcategories that didnt exist at that time')
    cohort_data = filter_old_dates(cohort_data, const.RECENT_SUBCATEGORIES, const.RECENT_DATES)

    logging.info('Add subcategory encoding')
    cohort_data = cohort_data.merge(
        subcategory_data,
        on='subcategory_name',
        how='inner'
    )

    logging.info('Create and merge cohort id')
    cohort_map = create_cohort_idx(cohort_data)
    cohort_data = cohort_data.merge(
        cohort_map,
        on='start_date',
        how='left'
    )

    # New features
    cohort_data['month_c'] = cohort_data.cancellation_date.dt.month
    cohort_data['month_s'] = cohort_data.start_date.dt.month
    cohort_data['year_c'] = cohort_data.cancellation_date.dt.year
    cohort_data['year_s'] = cohort_data.start_date.dt.year
    cohort_data['season_c'] = (cohort_data.month_c % 12 + 3) // 3
    cohort_data['season_s'] = (cohort_data.month_s % 12 + 3) // 3

    n_acquired_subscriptions = get_past_data_aggregations(final_data, subcategory_list,
                                                          train_test_cutoff_date)

    logging.info('Merge the number of subscriptions')
    cohort_data = cohort_data.merge(
        n_acquired_subscriptions,
        left_on=['start_date', 'subcategory_name'],
        right_on=['date_month', 'subcategory_name'],
        how='left'
    )

    ts_forecast = predict_ts_target(cohort_data, subcategory_list, train_test_cutoff_date)

    cohort_data = cohort_data.merge(
        ts_forecast[['ds', 'trend', 'yhat', 'subcategory_name']].rename(
            columns={'ds': 'cancellation_date'}),
        how='left',
        on=['cancellation_date', 'subcategory_name']
    )
    cohort_data.loc[cohort_data.yhat < 0, 'yhat'] = 0
    cohort_data['yhat'] = cohort_data.yhat.round()

    logging.info('Split data to speed up the computations')
    prophet_data = cohort_data[~cohort_data.subcategory_name.isin(const.RF_SUBCATEGORIES)]
    cohort_data = cohort_data[cohort_data.subcategory_name.isin(const.RF_SUBCATEGORIES)]

    # Targets
    if use_targets:
        daily_targets, monthly_targets = query_targets(country=const.COUNTRY, customer_type='B2C')
    else:
        monthly_targets = query_dynamic_targets(country=const.COUNTRY_ISO, customer_type='B2C')

    cohort_data = cohort_data.merge(
        monthly_targets.rename(
            columns={'n_subscriptions': 'yhat_n_subscriptions', 'asv': 'yhat_n_asv'}),
        how='left',
        on=['start_date', 'subcategory_name']
    )

    # Lag features
    cohort_data['cancellation_date_lag_1_year'] = cohort_data.cancellation_date.apply(
        lambda x: x + relativedelta(months=12))

    cohort_data = compute_lag_variables(
        cohort_data, False, 'cancellation_date_lag_1_year',
        'y_lag_1y', 'cancellation_date_lag_1_year_lag', 'y',
        ['cancellation_date', 'subcategory_name', 'effective_duration'],
        ['cancellation_date_lag_1_year_lag', 'subcategory_name', 'effective_duration'],
    )

    cohort_data['cancellation_date_lag_1_month'] = cohort_data.cancellation_date.apply(
        lambda x: x + relativedelta(months=1))

    cohort_data = compute_lag_variables(
        cohort_data, False, 'cancellation_date_lag_1_month',
        'y_lag_1m', 'cancellation_date_lag_1_month_lag', 'y',
        ['cancellation_date', 'subcategory_name', 'effective_duration'],
        ['cancellation_date_lag_1_month_lag', 'subcategory_name', 'effective_duration'],
    )

    cohort_data = compute_lag_variables(
        cohort_data, False, 'cancellation_date_lag_1_month',
        'yhat_lag_1m', 'cancellation_date_lag_1_month_lag', 'yhat',
        ['cancellation_date', 'subcategory_name', 'effective_duration'],
        ['cancellation_date_lag_1_month_lag', 'subcategory_name', 'effective_duration'],
    )

    cohort_data['start_date_lag_1_month'] = cohort_data.start_date.apply(
        lambda x: x + relativedelta(months=1))

    cohort_data = compute_lag_variables(
        cohort_data, True, 'start_date_lag_1_month',
        'yhat_n_subscriptions_lag1m', 'start_date_lag_1_month_lag', 'yhat_n_subscriptions',
        ['start_date', 'subcategory_name'],
        ['start_date_lag_1_month_lag', 'subcategory_name'],
    )

    logging.info('Replace future values with target ones')
    cohort_data.loc[
        (cohort_data.start_date >= train_test_cutoff_date), 'n_cohort_subscriptions'] = np.floor(
        cohort_data['yhat_n_subscriptions'])

    cohort_data.loc[(cohort_data.start_date >= train_test_cutoff_date), 'cohort_asv'] = np.floor(
        cohort_data['yhat_n_asv'])

    # Lag variables can keep a strict sign ">" because we shouldn't use the target for
    # the current month, but the true value (since it's from the past month)
    cohort_data.loc[(cohort_data.start_date > train_test_cutoff_date), 'y_lag_1m'] = np.floor(
        cohort_data['yhat_lag_1m'])

    cohort_data.loc[(
                                cohort_data.start_date > train_test_cutoff_date),
                    'n_cohort_subscriptions_lag1m'] = np.floor(
        cohort_data['yhat_n_subscriptions_lag1m'])

    logging.info(
        'Fill missing values for the small subcategories that dont '
        'have subscriptions for some of the past months')
    for feature_name in ['n_cohort_subscriptions', 'cohort_asv', 'n_cohort_subscriptions_lag1m']:
        cohort_data.loc[(cohort_data.start_date < train_test_cutoff_date) & (
            cohort_data[feature_name].isna()), feature_name] = 0

    logging.info('Fill 1-year lag variable with 0')
    for feature_name in ['y_lag_1y', 'y_lag_1m', 'n_cohort_subscriptions_lag1m']:
        cohort_data.loc[cohort_data[feature_name].isna(), feature_name] = 0

    logging.info('Import holidays')
    years = [year for year in
             range(cohort_data.start_date.min().year, cohort_data.start_date.max().year + 1)]
    cohort_data = include_holidays(cohort_data, years, const.COUNTRY)

    logging.info('Split train and future data')
    cohort_data = pd.concat([cohort_data, prophet_data])
    future_data = cohort_data[(cohort_data.cancellation_date >= train_test_cutoff_date)]
    cohort_data = cohort_data[(cohort_data.cancellation_date < train_test_cutoff_date)]

    logging.info(
        'Project future acquisitions targets for the subcategories that use an RF model '
        'that doesn\'t support null values')
    n_subc_projections = predict_ts_targets(cohort_data,
                                            const.RF_SUBCATEGORIES,
                                            'n_cohort_subscriptions')
    asv_subc_projections = predict_ts_targets(cohort_data, const.RF_SUBCATEGORIES, 'cohort_asv')
    future_data = future_data.merge(
        n_subc_projections[['ds', 'yhat', 'subcategory_name']].rename(
            columns={'ds': 'start_date', 'yhat': 'yhat_targets'}),
        how='left',
        on=['start_date', 'subcategory_name']
    ).merge(
        asv_subc_projections[['ds', 'yhat', 'subcategory_name']].rename(
            columns={'ds': 'start_date', 'yhat': 'yhat_targets_asv'}),
        how='left',
        on=['start_date', 'subcategory_name']
    )
    future_data.loc[
        (future_data.subcategory_name.isin(const.RF_SUBCATEGORIES)) &
        (future_data.n_cohort_subscriptions.isna()),
        'n_cohort_subscriptions'] = future_data.yhat_targets
    future_data.loc[
        (future_data.subcategory_name.isin(const.RF_SUBCATEGORIES)) &
        (future_data.cohort_asv.isna()), 'cohort_asv'] = future_data.yhat_targets_asv

    avg_asv = final_data.groupby(
        ['subcategory_name', 'date_month']).subscription_value.mean().dropna().reset_index()

    return cohort_data, future_data, avg_asv
