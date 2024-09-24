import logging
import os

import pandas as pd
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

from plugins.utils.sql import read_sql_file


def kill_long_running_queries():
    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

    file_location = "sql/get_long_running_queries.sql"

    sql_query = read_sql_file(os.path.join(__location__, file_location))
    engine = RedshiftSQLHook('redshift_admin').get_sqlalchemy_engine()

    long_running_pids = pd.read_sql_query(sql_query, engine)

    if len(long_running_pids.index) > 0:
        for index, row in long_running_pids.iterrows():
            sql = f"select pg_terminate_backend ({row['pid']})"
            logging.info(f" Query from user: {row['user_name']} "
                         f" is running since {row['running_time']} minutes. "
                         f" Threshold is {row['execution_time']} minutes.")
            engine.execute(sql)
    else:
        logging.info('No long running queries to kill ')
