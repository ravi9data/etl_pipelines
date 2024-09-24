import concurrent
import logging
from concurrent.futures import ThreadPoolExecutor as PoolExecutor
from datetime import datetime

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import pandas as pd
import requests

from plugins.utils.connections import redshift_conn

from .variables import EANS_QUERY, UPCS_QUERY

# Get required connections
conn = rd.RedshiftSQLHook('redshift_default').get_sqlalchemy_engine()
rs_conn = redshift_conn('redshift_default')
# Get current date
current_date = datetime.now().date()


def call_rf_api(ean_upcs, rf_api, api_key):
    asin = 'NULL'
    try:
        api_result = requests.get(rf_api,
                                  {
                                      'api_key': api_key,
                                      'amazon_domain': 'amazon.com',
                                      'type': 'product',
                                      'gtin': ean_upcs
                                  }
                                  )
        response = api_result.json()
        if response['request_info']['success'] is not False:
            asin = response['product']['asin']
        else:
            asin = 'NULL'
    except Exception as ex:
        print(str(ex))
    return [ean_upcs, asin]


def get_asins(eans_upcs_list, rf_api, rf_secret_key):
    count = 0
    gtin_asins_list = []
    with PoolExecutor(max_workers=10) as executor:
        responses = {executor.submit(call_rf_api, url,
                                     rf_api, rf_secret_key): url for url in eans_upcs_list}
        for response in concurrent.futures.as_completed(responses):
            count += 1
            print(count)
            try:
                data = response.result()
                gtin_asins_list.append(data)
            except Exception as ex:
                print(str(ex))
    return gtin_asins_list


def asin_converter(api_config: str,
                   converter: str,
                   api_key: str
                   ):
    if converter == 'ean_to_asin':
        logging.info(f'query to get eans : {EANS_QUERY}')
        rs_conn.execute(EANS_QUERY)
        eans = rs_conn.fetchall()
        rs_conn.close()
        eans_list = [x[0] for x in eans]
        logging.info(f'The total amount of EANs to convert to ASIN are : {len(eans_list)}')
        eans_asins_list = get_asins(eans_list, api_config['api'], api_key)
        eans_asins_df = pd.DataFrame(eans_asins_list, columns=['ean', 'asin'])
        asins_count = eans_asins_df[eans_asins_df['asin'].ne('NULL')].shape[0]
        eans_asins_df['date'] = pd.Timestamp(current_date)
        eans_asins_df.to_sql(con=conn,
                             method='multi',
                             chunksize=5000,
                             schema='pricing',
                             name='ean_asin',
                             index=False,
                             if_exists='append')
        logging.info(f'eans converted to asins are: {asins_count}')

    elif converter == 'upcs_to_asin':
        logging.info(f'query to get upcs : {UPCS_QUERY}')
        rs_conn.execute(UPCS_QUERY)
        upcs = rs_conn.fetchall()
        rs_conn.close()
        upcs_list = [x[0] for x in upcs]
        logging.info(f'The total amount of UPCs to convert to ASIN are : {len(upcs_list)}')
        upcs_asins_list = get_asins(upcs_list, api_config['api'], api_key)
        upcs_asins_df = pd.DataFrame(upcs_asins_list, columns=['upcs', 'asin'])
        asins_count = upcs_asins_df[upcs_asins_df['asin'].ne('NULL')].shape[0]
        upcs_asins_df['date'] = pd.Timestamp(current_date)
        upcs_asins_df.to_sql(con=conn,
                             method='multi',
                             chunksize=5000,
                             schema='pricing',
                             name='upcs_asin',
                             index=False,
                             if_exists='append')
        logging.info(f'upcs converted to asins are: {asins_count}')
