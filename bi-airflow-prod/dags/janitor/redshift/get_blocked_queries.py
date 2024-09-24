import logging
import os

import pandas as pd
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from tabulate import tabulate

from plugins.slack_utils import send_notification
from plugins.utils.sql import read_sql_file


def get_blocked_queries():
    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

    file_location = "sql/get_blocked_queries.sql"

    sql_query = read_sql_file(os.path.join(__location__, file_location))
    engine = RedshiftSQLHook('redshift_admin').get_sqlalchemy_engine()

    blocked_queries = pd.read_sql_query(sql_query, engine)

    if len(blocked_queries.index) > 0:
        payload = tabulate(blocked_queries, tablefmt='grid', headers='keys', showindex=False)
        message = (f"Below tables are locked/blocked for more than 15 mins"
                   f"\n```{payload}```")
        send_notification(message, 'slack')
    else:
        logging.info('No blocked queries ')
