from datetime import date

import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from dateutil.relativedelta import relativedelta

from plugins.utils.df_tools import optimize_floats, optimize_ints
from plugins.utils.sql import read_sql_file


def get_data(exclude_last_3_months=True):
    if exclude_last_3_months:
        param = (date.today() + relativedelta(months=-3)).strftime("%Y-%m-%d")
    else:
        param = (date.today() + relativedelta(months=-1)).strftime("%Y-%m-%d")

    QUERY = read_sql_file("./dags/labels/sql/subscription_data.sql").format(
        sub_start_day=param
    )

    pg_hook = PostgresHook(
        postgres_conn_id="redshift",
    )
    conn = pg_hook.get_conn()

    data = pd.read_sql(QUERY, conn)

    data = data.merge(returned_allocations_(conn), on="customer_id", how="left")
    data = data.merge(returned_asset_status(conn), on="customer_id", how="left")

    data = optimize_floats(data)
    data = optimize_ints(data)

    conn.close()

    return data


def returned_allocations_(conn):
    QUERY = read_sql_file("./dags/labels/sql/returned_allocations.sql")
    return pd.read_sql(QUERY, conn)


def returned_asset_status(conn):
    QUERY = read_sql_file("./dags/labels/sql/returned_asset_status.sql")
    return pd.read_sql(QUERY, conn)
