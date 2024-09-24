import datetime as dt
import logging

import requests
from airflow import AirflowException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

logger = logging.getLogger(__name__)


def extract_exchange_rates():
    end_date = dt.datetime.now()
    start_date = end_date - dt.timedelta(days=1)
    url = "https://api.apilayer.com/exchangerates_data/timeseries"
    headers = {
        "apikey": Variable.get('exchange_rates_api_key', default_var=None)
    }
    params = {"start_date": start_date.strftime('%Y-%m-%d'),
              "end_date": end_date.strftime('%Y-%m-%d'),
              "base": "USD",
              "symbols": "EUR"}
    payload = {}
    response = requests.request("GET", url, headers=headers, data=payload, params=params)
    status_code = response.status_code
    if status_code == 200:
        result = response.json()
        rates = result['rates']
        conn = RedshiftSQLHook('redshift_default').get_sqlalchemy_engine()

        for key in result['rates'].keys():
            rate_ = rates[key]['EUR']
            del_sql = "delete from trans_dev.daily_exchange_rate where date_ = '" + key + "';"
            ins_sql = "insert into trans_dev.daily_exchange_rate " \
                      "(date_, currency, exchange_rate_eur) " \
                      "values ('" + key + "'," + "'USD'," + str(rate_) + ");"
            logging.info(ins_sql)
            conn.execute(del_sql)
            conn.execute(ins_sql)
    else:
        logging.error('API Query Failure: {0} - {1}'.format(response.status_code, response.reason))
        raise AirflowException
