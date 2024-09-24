import datetime as dt
import logging

import pandas as pd
import requests
from airflow import AirflowException
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

logger = logging.getLogger(__name__)


def extract_referral_codes():
    end_date = dt.datetime.now()
    start_date = end_date - dt.timedelta(days=1)
    url = "https://referral-service.eu-production.internal.grover.com/referralCodes"
    params = {"from": start_date.strftime('%Y-%m-%d'),
              "to": end_date.strftime('%Y-%m-%d')
              }
    payload = {}
    response = requests.request("GET", url, data=payload, params=params)
    status_code = response.status_code
    if status_code == 200:
        df = pd.DataFrame(response.json())
        conn = RedshiftSQLHook('redshift_default').get_sqlalchemy_engine()

        target_table = 'referral_eu_codes'
        target_schema = 'ods_finance'
        del_sql = f"delete from {target_schema}.{target_table} " \
                  f"where createdAt::date = current_date-1 "
        res = conn.execute(del_sql)
        logging.info(f"deleted {str(res)} rows from {target_schema}.{target_table}")
        df.to_sql(name=target_table, schema=target_schema, con=conn, if_exists='append',
                  method='multi', chunksize=3000, index=False)
    else:
        logging.error('API Query Failure: {0} - {1}'.format(response.status_code, response.reason))
        raise AirflowException


if __name__ == '__main__':
    extract_referral_codes()
