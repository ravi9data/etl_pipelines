import logging
import os

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import pandas as pd

from plugins.utils.salesforce import sf_bulk_update
from plugins.utils.sql import read_sql_file

logger = logging.getLogger(__name__)

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


def update_account(**kwargs):
    sql_query = read_sql_file(os.path.join(__location__, 'sql/account.sql'))

    logging.info('Fetch data from redshift')

    rs = rd.RedshiftSQLHook(redshift_conn_id='redshift_default')
    conn = rs.get_conn()
    df = pd.read_sql_query(sql_query, conn)

    sf_bulk_update(data=df,
                   sf_conn_id='salesforce_b2b',
                   object_name='Account',
                   batch_size=2000)


def update_lead(**kwargs):
    sql_query = read_sql_file(os.path.join(__location__, 'sql/lead.sql'))

    logging.info('Fetch data from redshift')

    rs = rd.RedshiftSQLHook(redshift_conn_id='redshift_default')
    conn = rs.get_conn()
    df = pd.read_sql_query(sql_query, conn)

    sf_bulk_update(data=df,
                   sf_conn_id='salesforce_b2b',
                   object_name='Lead',
                   batch_size=2000)
