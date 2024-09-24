import logging
import os

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import airflow.providers.postgres.hooks.postgres as pg
import pandas as pd

from plugins.utils.sql import read_sql_file

logger = logging.getLogger(__name__)


def run_upsert():
    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

    rs_hook = rd.RedshiftSQLHook(redshift_conn_id='redshift_default')

    sql_query = read_sql_file(os.path.join(__location__,
                                           'sql/get_products.sql'))
    pg_conn = pg.PostgresHook(postgres_conn_id='dwh_rds_replica').get_conn()
    df = pd.read_sql_query(sql_query, pg_conn)
    logger.info(f'Fetched {len(df.index)} from RDS database')

    if len(df.index) > 0:
        rs_conn = rs_hook.get_sqlalchemy_engine()
        df.to_sql(name='stg_product',
                  schema='ods_production',
                  if_exists='replace',
                  index=False,
                  con=rs_conn,
                  method='multi',
                  chunksize=2000)

        sql_query = read_sql_file(os.path.join(__location__,
                                               'sql/cleanup_products.sql'))
        rs_hook.run(sql_query, autocommit=True, parameters=None)

        sql_query = read_sql_file(os.path.join(__location__,
                                               'sql/insert_products.sql'))
        rs_hook.run(sql_query, autocommit=True, parameters=None)

    else:
        logging.info('No New records to update')
