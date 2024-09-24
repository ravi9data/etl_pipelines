import logging
import os

import pandas as pd
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

from plugins.utils.sql import read_sql_file

logger = logging.getLogger(__name__)


def run_vacuum():
    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

    file_location = "sql/get_vacuum_tables.sql"

    sql_query = read_sql_file(os.path.join(__location__, file_location))
    engine = RedshiftSQLHook('redshift_admin').get_sqlalchemy_engine()

    vacuum_tables_list = pd.read_sql_query(sql_query, engine)
    logger.info(f'Need to run vacuum for below tables \n {vacuum_tables_list}')

    conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")

    if len(vacuum_tables_list.index) > 0:
        for index, row in vacuum_tables_list.iterrows():
            analyze_sql = f"analyze {row['schema_name']}.{row['table_name']}"
            vacuum_sql = f"vacuum {row['schema_name']}.{row['table_name']}"
            logger.info(f" Running Analyze for : {row['schema_name']}.{row['table_name']}")
            conn.execute(analyze_sql)
            logger.info(f" Running Vacuum for : {row['schema_name']}.{row['table_name']}")
            conn.execute(vacuum_sql)
    else:
        logger.info('Vacuum and Stats are good. No maintenance required !')
