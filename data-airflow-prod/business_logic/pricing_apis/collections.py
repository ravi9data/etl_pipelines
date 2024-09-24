import logging
from datetime import date

import pandas as pd
from airflow.providers.amazon.aws.hooks.redshift import RedshiftSQLHook
from airflow.providers.trino.hooks.trino import TrinoHook

from business_logic.pricing_apis.api_utils import (add_requests,
                                                   create_collection,
                                                   start_collection)
from plugins.utils.sql import read_sql_file

COLLECTION_MAX_REQUESTS = 15000
MAX_PAGES = 10


def create_collections(
        api_key: str,
        api_config: str,
):
    api_domain = api_config['api_domain']
    destination_id = api_config['destination_id']
    sql_input_list = api_config['input_list_sql_template']

    redshift_engine = RedshiftSQLHook().get_sqlalchemy_engine()

    collection_id_list = []

    chunk_size = int(COLLECTION_MAX_REQUESTS / MAX_PAGES)

    for sql_input_item in sql_input_list:
        logging.info(f'running sql : {sql_input_item}')
        for data_chunk in pd.read_sql_query(con=redshift_engine,
                                            sql=read_sql_file(sql_input_item),
                                            chunksize=chunk_size
                                            ):
            logging.info(f'length of request data in collection : {data_chunk.index}')
            request_types = data_chunk["type"].unique().tolist()
            requests_type = None
            if len(request_types) == 1:
                requests_type = request_types[0]

            collection_name = str(date.today()).replace('-', '')
            collection_id = create_collection(
                collection_name,
                api_domain,
                api_key,
                destination_id,
                requests_type,
            )
            add_requests(data_chunk, collection_id, api_domain, api_key)

            collection_id_list.append(collection_id)

    logging.info(f'length of collection_id_list : {len(collection_id_list)} rows')

    for collection_id in collection_id_list:
        start_collection(collection_id, api_domain, api_key)
    return collection_id_list


def create_countdown_collections(
        api_key: str,
        api_config: str,
):
    api_domain = api_config['api_domain']
    destination_id = api_config['destination_id']

    trino_engine = TrinoHook().get_sqlalchemy_engine()

    collection_id_list = []

    chunk_size = int(COLLECTION_MAX_REQUESTS / MAX_PAGES)

    for data_chunk in pd.read_sql_query(con=trino_engine,
                                        sql=read_sql_file(api_config['input_list_sql_template']),
                                        chunksize=chunk_size
                                        ):
        logging.info(f'length of request data in collection : {data_chunk.index}')
        request_types = data_chunk["type"].unique().tolist()
        requests_type = None
        if len(request_types) == 1:
            requests_type = request_types[0]

        collection_name = str(date.today()).replace('-', '')
        collection_id = create_collection(
            collection_name,
            api_domain,
            api_key,
            destination_id,
            requests_type,
        )
        add_requests(data_chunk, collection_id, api_domain, api_key)

        collection_id_list.append(collection_id)

    for collection_id in collection_id_list:
        start_collection(collection_id, api_domain, api_key)
    return collection_id_list
