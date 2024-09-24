import calendar
import logging

import holidays
import numpy as np
import pandas as pd
from prophet import Prophet

logger = logging.getLogger(__name__)


# This function check for the gaps in a given date range, and add new rows corresponding to the
# missing dates.
# This is important to fill past data, and also to generate future data.
def fill_past_missing_data(subcategory_list, start_date, end_date, cohort_data):
    for subcat in subcategory_list:

        for cohort in pd.date_range(start_date, end_date, freq='MS').tolist():
            cohort_data = cohort_data.append(pd.DataFrame(list(
                set(pd.date_range(cohort, end_date, freq='MS').tolist()) -
                set(cohort_data[(cohort_data.start_date == cohort) &
                                (cohort_data.subcategory_name == subcat)].
                    cancellation_date.tolist()))).rename(columns={0: 'cancellation_date'}))

            cohort_data.reset_index(drop=True, inplace=True)

            cohort_data['start_date'] = cohort_data['start_date'].fillna(cohort)
            cohort_data['y'] = cohort_data['y'].fillna(0)
            cohort_data['subcategory_name'] = cohort_data['subcategory_name'].fillna(subcat)

    cohort_data['effective_duration'] = cohort_data.groupby(
        ['start_date', 'subcategory_name']).cancellation_date.rank() - 1

    return cohort_data


# This function assign an id to each cohort date. It starts from id=0,
# which corresponds to January 2019.
def create_cohort_idx(cohort_data):
    cohort_mapping = dict.fromkeys(cohort_data.start_date.unique(), np.nan)

    for i, k in enumerate(cohort_mapping):
        cohort_mapping[k] = i

    data = pd.DataFrame(cohort_mapping.items(), columns=['start_date', 'cohort_idx'])

    return data


# This function extract the date in the format: YYYY-MM-01,
# so that every date can be expressed on a monthly basis.
def extract_date(data, date_name, names):
    data[names[0]] = data[date_name].dt.day.copy()
    data[names[1]] = data[date_name].dt.month.copy()
    data[names[2]] = data[date_name].dt.year.copy()

    data[names[3]] = pd.to_datetime(
        data[[names[1], names[2]]].rename(columns={names[1]: 'MONTH', names[2]: 'YEAR'}).assign(
            DAY=1))
    return data


# This function filters out old data.
# We say it's old if the date is before a defined threshold,
# which could be different per each subcategory.
def filter_old_dates(data, subcat, start_date):
    for s, d in zip(subcat, start_date):
        data = data[~((data.start_date < d) & (data.subcategory_name == s))]

    return data


# This function computes and merges the number of weekend and weekdays per month
def extract_n_weekdays(cohort_data):
    cancellation_dates_ts = pd.Series(cohort_data.cancellation_date.unique())

    weekdays_data = pd.DataFrame(
        {'cancellation_date': cancellation_dates_ts.rename('cancellation_date'),
         'month_days': pd.Series(
             [calendar.monthrange(x.year, x.month)[1] for x in cancellation_dates_ts]).rename(
             'month_days'), 'n_canc_month_saturdays': cancellation_dates_ts.apply(
            lambda x: len([1 for i in calendar.monthcalendar(x.year, x.month) if i[5] != 0])),
         'n_canc_month_sundays': cancellation_dates_ts.apply(lambda x: len(
             [1 for i in calendar.monthcalendar(x.year, x.month) if i[6] != 0])), })

    weekdays_data['n_canc_month_weekendays'] = \
        weekdays_data['n_canc_month_sundays'] + weekdays_data['n_canc_month_saturdays']
    weekdays_data['n_canc_month_weekdays'] = \
        weekdays_data['month_days'] - weekdays_data['n_canc_month_weekendays']

    cohort_data = cohort_data.merge(
        weekdays_data[['cancellation_date', 'n_canc_month_weekendays', 'n_canc_month_weekdays']],
        on='cancellation_date', how='left')

    return cohort_data


# This function add information about the bank holidays of a country,
# and returns the input data with additional columns.
def include_holidays(cohort_data, years, country):
    holidays_data = holidays.country_holidays(country=country, years=years)
    holidays_data = pd.DataFrame(holidays_data.items())
    holidays_data.columns = ['date_holiday', 'holiday']
    holidays_data.date_holiday = pd.to_datetime(holidays_data.date_holiday)

    holidays_data = extract_date(
        holidays_data,
        'date_holiday',
        ['subscription_day', 'date_month', 'date_year', 'cancellation_date']
    )

    holidays_data = holidays_data.groupby('cancellation_date')[['holiday']].count()

    # Extract holidays corresponding to the cohort start date and the cancellation date:
    cohort_data = cohort_data.merge(
        holidays_data,
        on='cancellation_date',
        how='left'
    ).merge(
        holidays_data,
        left_on='start_date',
        right_on='cancellation_date',
        how='left'
    )
    cohort_data = cohort_data.rename(
        columns={'holiday_x': 'holiday_canc', 'holiday_y': 'holiday_start'})
    cohort_data.holiday_canc = cohort_data.holiday_canc.fillna(0)
    cohort_data.holiday_start = cohort_data.holiday_start.fillna(0)

    return cohort_data


# This function fills null values
def fill_na_values(cohort_data, max_target_date, train_test_cutoff_date, use_targets):
    # Fill missing values for the small subcategories that
    # don't have subscriptions for some of the past months
    cohort_data.loc[(cohort_data.start_date < train_test_cutoff_date) & (
        cohort_data.n_cohort_subscriptions.isna()), 'n_cohort_subscriptions'] = 0
    cohort_data.loc[(cohort_data.start_date < train_test_cutoff_date) & (
        cohort_data.cohort_asv.isna()), 'cohort_asv'] = 0

    if use_targets:
        # Fill missing values for the small subcategories
        # that don't have subscriptions for some of the past months
        cohort_data.loc[(cohort_data.start_date < train_test_cutoff_date) & (
            cohort_data.n_subs_first_week_of_month.isna()), 'n_subs_first_week_of_month'] = 0
        cohort_data.loc[(cohort_data.start_date < train_test_cutoff_date) & (
            cohort_data.n_subs_last_week_of_month.isna()), 'n_subs_last_week_of_month'] = 0

        # Fill missing values (that correspond to future dates) with targets
        cohort_data.n_subs_first_week_of_month = cohort_data.n_subs_first_week_of_month.fillna(
            cohort_data.n_subs_first_week_of_month_target)
        cohort_data.n_subs_last_week_of_month = cohort_data.n_subs_last_week_of_month.fillna(
            cohort_data.n_subs_last_week_of_month_target)

        # Fill null values with 0 until the last available target date.
        # For later dates, we don't have the value and we can keep it null.
        cohort_data.loc[(cohort_data.n_subs_last_week_of_month.isna()) & (
                cohort_data.cancellation_date <= max_target_date), 'n_subs_last_week_of_month'] = 0
        cohort_data.loc[(cohort_data.n_subs_first_week_of_month.isna()) & (
                cohort_data.cancellation_date <= max_target_date), 'n_subs_first_week_of_month'] = 0

    cohort_data.loc[(cohort_data.n_cohort_subscriptions.isna()) & (
            cohort_data.cancellation_date <= max_target_date), 'n_cohort_subscriptions'] = 0

    # Fill 1-year lag variable with 0
    cohort_data.loc[cohort_data.y_lag1y.isna(), 'y_lag1y'] = 0

    return cohort_data


def aggregate_subscriptions(final_data, start_date, month_cutoff, subcategory_list):
    logging.info('Start the aggregation')
    cohort_data = final_data.groupby(
        ['date_month', 'cancellation_month', 'subcategory_name']).size().reset_index()
    logging.info('Rename columns')
    cohort_data.columns = ['start_date', 'cancellation_date', 'subcategory_name', 'y']

    logging.info("Reduce dimensionality for data that we don't need any more because "
                 "it's too old and there are no more cancellations coming from it.")
    cohort_data = cohort_data[cohort_data.start_date >= start_date]

    logging.info("Remove cancellations data if the current month has not been completed yet")
    # (we need a full month data to define the aggregated value per month).
    cohort_data = cohort_data[cohort_data.cancellation_date.dt.date < month_cutoff]

    logging.info("Remove weird cases: there shouldn't be cancellations happening "
                 "before the start of a subscription")
    cohort_data = cohort_data[cohort_data.start_date <= cohort_data.cancellation_date]

    logging.info("Only consider the subcategories that are active")
    cohort_data = cohort_data[cohort_data.subcategory_name.isin(subcategory_list)]

    return cohort_data


def predict_ts_target(cohort_data, subcategory_list, train_test_cutoff_date):
    ts_forecast = pd.DataFrame()

    for s in subcategory_list:
        X = cohort_data[cohort_data.subcategory_name == s].groupby(['cancellation_date'])[
            ['y']].sum().reset_index().rename(
            columns={'cancellation_date': 'ds'}
        )
        prophet = Prophet()
        prophet.fit(X[X.ds < train_test_cutoff_date])
        future = prophet.make_future_dataframe(periods=12, freq="MS")
        forecast = prophet.predict(future)

        ts_forecast = pd.concat([
            ts_forecast,
            pd.concat([
                forecast.reset_index(drop=True),
                pd.Series([s] * (forecast.shape[0])).rename('subcategory_name')
            ], axis=1)])

    return ts_forecast


def compute_lag_variables(
        cohort_data,
        merge_data_acquisitions,
        lag_date,
        new_lag_name,
        new_lag_date_name,
        target_feature,
        left_join_feats,
        right_join_feats,
):
    if merge_data_acquisitions:
        merge_data = cohort_data.groupby(['start_date_lag_1_month', 'subcategory_name'])[
            ['yhat_n_subscriptions']].min().reset_index()
    else:
        merge_data = cohort_data[[
            lag_date,
            target_feature,
            'subcategory_name',
            'effective_duration'
        ]].drop_duplicates()

    cohort_data = cohort_data.merge(
        merge_data.rename(
            columns={
                target_feature: new_lag_name,
                lag_date: new_lag_date_name
            }),
        left_on=left_join_feats,
        right_on=right_join_feats,
        how='left',
    )

    return cohort_data
