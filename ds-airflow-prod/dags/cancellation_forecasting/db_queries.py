import logging
from datetime import datetime

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import pandas as pd

from dags.cancellation_forecasting.utils import extract_date
from plugins.utils.sql import read_sql_file

logger = logging.getLogger(__name__)


def query_subscriptions(
        time_cutoff=datetime.today().date().replace(day=1),
        country_name='Germany',
        store='Grover Germany',
        customer_type='normal_customer'
):
    logging.info("Import subscriptions")
    subs_sql = read_sql_file("./dags/cancellation_forecasting/sql/fetch_subscriptions.sql").format(
        country_name=country_name, customer_type=customer_type, store=store)
    rs = rd.RedshiftSQLHook(redshift_conn_id='redshift')
    redshift_engine = rs.get_sqlalchemy_engine()

    logging.info(subs_sql)

    data = pd.read_sql(
        subs_sql,
        redshift_engine
    )
    logging.info('Subscriptions row count : {0}'.format(len(data.index)))

    logging.info("Start subscription: extract monthly dates")
    data = extract_date(
        data,
        'subscription_start_date',
        ['subscription_day', 'subscription_month', 'subscription_year', 'date_month']
    )  # subscription_day was subscription_date

    logging.info("End subscription: extract monthly dates")
    data = extract_date(
        data,
        'cancellation_date',
        ['cancellation_date_', 'cancellation_month', 'cancellation_year', 'cancellation_month']
    )

    data = data[data.subscription_start_date.dt.date < time_cutoff]  # timezones UTC vs UTC

    return data


def query_targets(country='Germany', customer_type='B2C'):
    # Import targets
    rs = rd.RedshiftSQLHook(redshift_conn_id='redshift')
    redshift_engine = rs.get_sqlalchemy_engine()

    targets_sql = read_sql_file("./dags/cancellation_forecasting/sql/fetch_targets.sql").format(
        country=country, customer_type=customer_type)

    logging.info(targets_sql)

    daily_targets = pd.read_sql(
        targets_sql,
        redshift_engine,
        parse_dates=['datum']
    )
    logging.info("Start subscription: extract monthly dates")
    daily_targets = extract_date(
        daily_targets,
        'datum',
        ['subscription_day', 'date_month', 'date_year', 'start_date']
    )

    while daily_targets[daily_targets.datum == daily_targets.datum.max()].n_subscriptions.sum() \
            == 0:
        daily_targets = daily_targets[daily_targets.datum != daily_targets.datum.max()]

    logging.info("Get monthly targets")
    monthly_targets = daily_targets.copy()

    monthly_targets = monthly_targets.groupby(
        ['start_date', 'subcategory_name']
    )[['n_subscriptions', 'asv']].sum().reset_index(
        drop=False
    )

    return daily_targets, monthly_targets


def query_dynamic_targets(country='DE', customer_type='B2C'):
    # Import targets
    rs = rd.RedshiftSQLHook(redshift_conn_id='redshift')
    redshift_engine = rs.get_sqlalchemy_engine()

    targets_sql = read_sql_file(
        "./dags/cancellation_forecasting/sql/fetch_dynamic_targets.sql"
        ).format(
        country=country, customer_type=customer_type)

    logging.info(targets_sql)

    targets = pd.read_sql(
        targets_sql,
        redshift_engine,
        parse_dates=['start_date']
        )

    # To avoid issues with B2B/Freelancers and B2B/Non-Freelancers
    targets = (
        targets
        .groupby(['start_date', 'subcategory_name', 'index'])
        .value
        .sum()
        .reset_index()
        )

    # Set table as official targets
    targets = targets.pivot(values='value',
                            index=['start_date', 'subcategory_name'],
                            columns='index').reset_index()
    targets = targets.rename(
        columns={
            'Acquired ASV': 'asv',
            'Acquired Subscriptions': 'n_subscriptions'
        }
    )

    return targets
