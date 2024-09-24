import logging
import os

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import sqlparse

from plugins.slack_utils import send_notification

logger = logging.getLogger(__name__)


def run_marketing_costs_daily_importer():

    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

    rs = rd.RedshiftSQLHook(redshift_conn_id='redshift_default')
    rs_conn = rs.get_conn()

    for filename in os.listdir(__location__+'/sql'):
        file_path = __location__ + '/sql/' + filename
        logger.info(f'Executing {file_path}')
        with open(file_path, 'r') as file:
            raw_sql_file = file.read()
            sql_stmts = sqlparse.split(raw_sql_file)
            for stmt in sql_stmts:
                try:
                    logger.info(stmt)
                    rs_conn.run(sql=stmt, autocommit=True)
                    rs_conn.commit()
                except Exception as e:
                    err_msg = f'Error occurred in marketing pipeline while executing {filename}' \
                              f'\n```{repr(e)}```'
                    rs_conn.rollback()
                    logger.error(err_msg)
                    send_notification(err_msg, 'slack')
                    continue
    rs_conn.close()
