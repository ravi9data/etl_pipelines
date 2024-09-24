import asyncio
import json
import logging
from datetime import datetime
from time import sleep

import pandas as pd
import toolz
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from airflow.models import Variable
from awswrangler.athena import read_sql_query, read_sql_table
from awswrangler.s3 import to_parquet

from plugins.date_utils import requests_date_handler
from plugins.utils.redshift import get_alchemy_connection
from plugins.utils.requests_utils import xratelimit_remaining
from plugins.utils.sql import read_sql_file

logger = logging.getLogger(__name__)

CONN_ID = 'redshift'


def joined_user_product_data_write_to_s3():
    """
    Join user and product data and writes it to S3
    """

    reverse_etl_bucket = Variable.get('reverse_etl_bucket')
    database = Variable.get('reverse_etl_database')
    workgroup = Variable.get('athena_workgroup', default_var='deng-applications')

    query = read_sql_file(
        './dags/reverse_etl/braze/price_drop/sql/athena_product_data.sql')

    logger.info('Loading Product Data in pandas DataFrame')
    df_products = read_sql_query(
        sql=query,
        database=Variable.get('api_production_target_glue_database'),
        workgroup=workgroup,
        params={
            "image_path": "'image/upload/'",
            "emptystring": "''",
            "store_type": "'1.0'",
            "code": "'de','at','nl','es','us'",
            "availability": "'available'"
            })
    logger.info('Product Data loaded in pandas DataFrame from Athena')

    logger.info('Loading User Data in pandas DataFrame from Redshift')
    df_users = pd.read_sql_table(
                con=get_alchemy_connection(CONN_ID),
                table_name='v_braze_user_price_drop',
                schema='marketing')
    logger.info('User Data loaded in pandas DataFrame')

    logger.info('Joining User and Product Data')
    df_join = pd.merge(
        df_users,
        df_products,
        how='inner',
        left_on=['product_sku', 'store_id'],
        right_on=['product_sku', 'store_id'])
    logger.info('Joined User and Product Data')

    logger.info('Writing Joined Data to S3')

    to_parquet(
        df=df_join,
        path=f's3://{reverse_etl_bucket}/braze/price_drop_join_user_product_data',
        index=False,
        dataset=True,
        database=database,
        table='braze_price_drop_user_and_product',
        mode='overwrite',
        dtype={
              'customer_id': 'bigint',
              'has_active_subscription_with_product': 'bigint',
              'plan_duration': 'float',
              'price': 'float',
              'product_name_x': 'string',
              'product_sku': 'string',
              'quantity': 'float',
              'slug': 'string',
              'source_': 'string',
              'store_id': 'string',
              'store_label': 'string',
              'updated_at_x': 'string',
              'variant_id_x': 'string',
              'product_id': 'string',
              'product_name_y': 'string',
              'product_slug': 'string',
              'rental_plan_id': 'string',
              'rental_plan_active': 'string',
              'variant_id_y': 'string',
              'availability': 'string',
              'current_price': 'string',
              'old_price': 'string',
              'price_diff': 'float',
              'image_url': 'string',
              'updated_at_y': 'string'}
    )
    logger.info('Joined Data Written to S3')


def aggregate_joined_data_write_to_s3():
    """
    Using the output of joined_user_product_data_write_to_s3, aggregate the data
    using the query in dags/braze/price_drop/sql/braze_price_drop_agg.sql and writes the
    result to S3
    """

    reverse_etl_bucket = Variable.get('reverse_etl_bucket')
    database = Variable.get('reverse_etl_database')
    workgroup = Variable.get('athena_workgroup', default_var='deng-applications')

    query = read_sql_file(
        './dags/reverse_etl/braze/price_drop/sql/braze_price_drop_agg.sql')

    logger.info('Executing Query in Athena')
    df_agg = read_sql_query(
        sql=query,
        database=database,
        workgroup=workgroup
        )
    logger.info('Query executed successfully in Athena')

    logger.info('Writing query result to S3')

    tables_to_write_and_mode = [
        {'table': 'braze_price_drop_agg', 'mode': 'overwrite'},
        {'table': 'braze_price_drop_agg_historical', 'mode': 'append'},
        ]

    for table_and_mode in tables_to_write_and_mode:
        logger.info(f'Writting result to table {table_and_mode["table"]} '
                    f'with mode {table_and_mode["mode"]}')
        if table_and_mode["table"] == 'braze_price_drop_agg_historical':
            df_agg['updated_at'] = datetime.utcnow()
        to_parquet(
            df=df_agg,
            path=f's3://{reverse_etl_bucket}/braze/{table_and_mode["table"]}',
            index=False,
            dataset=True,
            database=database,
            table=table_and_mode["table"],
            mode=table_and_mode["mode"],
            dtype={
                'mtl_cart_price_drop_names': 'array<string>',
                'mtl_cart_price_drop_full_prices': 'array<string>',
                'mtl_cart_price_drop_sale_prices': 'array<string>',
                'mtl_cart_price_drop_images': 'array<string>',
                'mtl_cart_price_drop_links': 'array<string>',
                'mtl_cart_price_drop_variant_sku': 'array<string>',
                'mtl_wishlist_price_drop_names': 'array<string>',
                'mtl_wishlist_price_drop_full_prices': 'array<string>',
                'mtl_wishlist_price_drop_sale_prices': 'array<string>',
                'mtl_wishlist_price_drop_images': 'array<string>',
                'mtl_wishlist_price_drop_links': 'array<string>',
                'mtl_wishlist_price_drop_variant_sku': 'array<string>'}
        )
        logger.info('Result written successfully to table '
                    f'{table_and_mode["table"]} with mode {table_and_mode["mode"]}')


async def post_records(records, session):
    """
    Generic Function to send data to Braze using aiohttp module.
    :param records: dict
    :param session: tcp session
    :return: int, the status code of the sending result
    """
    braze_api_key = Variable.get('braze_api_key')
    braze_url = Variable.get('braze_url')

    header = {'Authorization': "Bearer " + braze_api_key, 'Content-Type': 'application/json'}

    header_limit = 20

    try:
        async with session.post(
                braze_url,
                headers=header,
                data=json.dumps({"attributes": records},
                                default=requests_date_handler)) as resp:
            logger.info(f'Request sent with status: {resp.status},{await resp.text()}')
            res_headers = resp.headers
            if xratelimit_remaining(response_limit=res_headers["X-RateLimit-Remaining"],
                                    limit=header_limit):

                now = datetime.utcnow()
                reset = datetime.utcfromtimestamp(int(res_headers["X-RateLimit-Reset"]))
                delta = reset - now
                delta_in_sec = delta.seconds
                logger.info(f'The remaining header limit is below {header_limit} '
                            f'APIs calls will resume after {delta_in_sec} seconds'
                            'Once the current rate limit window resets')
                sleep(delta_in_sec)

            return resp.status
    except Exception as error:
        logger.error(error)
        return 999


async def async_send_data(data_to_send):
    """
    Wrapper function that receives data to send to Braze and makes asynch posts.
    Args:
        data_to_send (list): list of records

    Raises:
        Error rate rate too high:  If error rate above 1%

    Returns:
        list: list of response status from API request
    """
    logger.info('Getting event loop')
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)

    connector = TCPConnector(limit=50, limit_per_host=5)

    logger.info('Settting time out of ClientSession to 30 minutes')
    timeout = ClientTimeout(total=60*30)
    session = ClientSession(connector=connector, timeout=timeout)

    loop_future = [await loop.run_in_executor(None, post_records, batch_to_post, session)
                   for batch_to_post in data_to_send]

    logger.info('starting send')
    results = []
    for my_function_result in await asyncio.gather(*loop_future):
        results.append(my_function_result)
    logger.info('Finished send')

    total_posts = len(results)
    errors = [res for res in results if res not in (200, 201)]
    logger.info(f'We have post {total_posts} times')
    error_rate = round(len(errors) / total_posts, 3)

    error_rate_threshold = 0.05
    if error_rate >= error_rate_threshold:
        logging.error(f'Error rate of {error_rate * 100}% above '
                      f'{error_rate_threshold * 100}%')
        raise NameError(f'Error rate too high:{error_rate * 100}% '
                        'above {error_rate_threshold * 100}%')
    else:
        logger.info(f'Error rate of {len(errors)}/{total_posts}')


def build_record(row):
    """
    Receives a row from a DataFrame as an argument and creates a dictonary to be
    sent to braze.
    Args:
        row (dict): row from a dataframe as a dictionary

    Returns:
        dict : record to be sent to braze.
    """

    current_ts = str(datetime.utcnow())
    record = {
        "external_id": row['external_id'],
        "_update_existing_only": 'true'}
    if row["mtl_cart_price_drop_names"] is not None:
        record.update({
            "mtl_cart_price_drop_names": list(row['mtl_cart_price_drop_names']),
            "mtl_cart_price_drop_full_prices": [
                f'{val}_{int(idx)+1}'
                for idx, val in enumerate(row['mtl_cart_price_drop_full_prices'])],
            "mtl_cart_price_drop_sale_prices": [
                f'{val}_{int(idx)+1}'
                for idx, val in enumerate(row['mtl_cart_price_drop_sale_prices'])],
            "mtl_cart_price_drop_images": list(row['mtl_cart_price_drop_images']),
            "mtl_cart_price_drop_links": list(row['mtl_cart_price_drop_links']),
            "mtl_cart_price_drop_variant_sku": list(row['mtl_cart_price_drop_variant_sku']),
            "mtl_price_drop_updated_at": current_ts
                })
    if row["mtl_wishlist_price_drop_names"] is not None:
        record.update({
            "mtl_wishlist_price_drop_names": list(row['mtl_wishlist_price_drop_names']),
            "mtl_wishlist_price_drop_full_prices":
                [f'{val}_{int(idx)+1}'
                    for idx, val in enumerate(row['mtl_wishlist_price_drop_full_prices'])],
            "mtl_wishlist_price_drop_sale_prices":
                [f'{val}_{int(idx)+1}'
                    for idx, val in enumerate(row['mtl_wishlist_price_drop_sale_prices'])],
            "mtl_wishlist_price_drop_images": list(row['mtl_wishlist_price_drop_images']),
            "mtl_wishlist_price_drop_links": list(row['mtl_wishlist_price_drop_links']),
            "mtl_wishlist_price_drop_variant_sku": list(row['mtl_wishlist_price_drop_variant_sku']),
            "mtl_price_drop_updated_at": current_ts
            })

    return record if len(record) > 2 else None


def get_data_to_send(df, record_builder, batch_size=50):
    """
    Receives a DataFrame and a record_builder function which is
    applied to the DataFrame row by row and returns a new column
    to the dataframe named 'records'

    After that the column 'records' is passed as a list to
    toolz.partition_all function which create a list of list where
    length of each list is the batch_size.

    Args:
        df (pd.DataFrame): data from a Glue table
        record_builder (python function): to build the record that will be sent to braze.
        batch_size (int, optional): Defaults to 50.

    Returns:
        list[list]: Records to send.
    """

    logger.info(f'DataFrame length is {len(df.index)}')

    logger.info('Applying function build_record to DataFrame')

    df['records'] = df.apply(record_builder, axis=1)

    df = df[~pd.isna(df['records'])]

    logger.info(f'DataFrame length without None records: {len(df.index)}')

    total_records = toolz.partition_all(batch_size, df['records'].to_list())

    return total_records


def send_data_wrapper(batch_size=50):
    """
    Wrapper function that load data, build and add the records and send them async
    Args:
        batch_size (int, optional): Defaults to 50.

    """

    database = Variable.get('reverse_etl_database')

    logger.info('Loading table data_production_reverse_etl.braze_price_drop_agg into '
                'DataFrame')
    df_agg_to_send = read_sql_table(
        table="braze_price_drop_agg",
        database=database)

    total_records = get_data_to_send(df=df_agg_to_send,
                                     record_builder=build_record,
                                     batch_size=batch_size)

    asyncio.run(async_send_data(data_to_send=total_records))


def diff_and_deltas(row):
    """
    compares the products in the cart and wishlist for every customer in the two last previous runs.
    """
    sources = ['wishlist', 'cart']

    for source in sources:
        if row[f'mtl_{source}_price_drop_names_last'] is None:
            row[f'mtl_{source}_price_drop_names_last'] = []
        if row[f'mtl_{source}_price_drop_names_prev'] is None:
            row[f'mtl_{source}_price_drop_names_prev'] = []
        last_set = set(row[f'mtl_{source}_price_drop_names_last'])
        prev_set = set(row[f'mtl_{source}_price_drop_names_prev'])
        diff = list(prev_set.symmetric_difference(last_set))

        list_of_index = [
            index
            for element in diff
            for index, value in enumerate(row[f'mtl_{source}_price_drop_names_last'])
            if value == element]

        if len(list_of_index) > 0:
            row[f'mtl_price_drop_{source}_delta_indexes'] = list(map(str, sorted(list_of_index)))
        else:
            row[f'mtl_price_drop_{source}_delta_indexes'] = ['no_deltas']

        row[f'mtl_price_drop_{source}_delta_indexes_update_at'] = str(datetime.utcnow())

    return row


def calculate_deltas_write_to_s3():
    """
    Loads data from data_production_reverse_etl.braze_price_drop_agg_historical.
    Executes ROW_NUMBER OVER PARTITION external_id ORDER BY updated_at DESC.
    Gets the last 2 runs (calling them last_run and prev_run).
    Joins last_run and prev_run.
    With the resulting Dataframe deltas are calculate_delta.
    Deltas are written to s3.
    """

    database = Variable.get('reverse_etl_database')
    reverse_etl_bucket = Variable.get('reverse_etl_bucket')
    workgroup = Variable.get('athena_workgroup', default_var='deng-applications')

    logger.info('Loading data_production_reverse_etl.braze_price_drop_agg_historical query'
                'into DataFrame.')

    query = read_sql_file(
        './dags/reverse_etl/braze/price_drop/sql/athena_last_2_runs.sql')

    deltas_df = read_sql_query(
        sql=query,
        database=database,
        workgroup=workgroup)

    logger.info('Calculating deltas')
    deltas_df = deltas_df.apply(diff_and_deltas, axis=1)

    logger.info('Removing unnecessary columns')
    deltas_df = deltas_df[[
                        'external_id',
                        'mtl_price_drop_wishlist_delta_indexes',
                        'mtl_price_drop_wishlist_delta_indexes_update_at',
                        'mtl_price_drop_cart_delta_indexes',
                        'mtl_price_drop_cart_delta_indexes_update_at'
                        ]]

    logger.info('Writing deltas to S3')
    to_parquet(
        df=deltas_df,
        path=f's3://{reverse_etl_bucket}/braze/braze_price_drop_deltas',
        index=False,
        dataset=True,
        database=database,
        table='braze_price_drop_deltas',
        mode='overwrite',
        dtype={
            'external_id': 'string',
            'mtl_price_drop_wishlist_delta_indexes':  'array<string>'
            })


def build_record_deltas(row):
    """
    Receives a row from a DataFrame as an argument and creates a dictonary to be
    sent to braze.
    Args:
        row (dict): row from a dataframe as a dictionary

    Returns:
        dict : record to be sent to braze if there are deltas if not None
    """

    current_ts = str(datetime.utcnow())

    record = {
        "external_id": row['external_id'],
    }

    if row["mtl_price_drop_cart_delta_indexes"][0] != 'no_deltas':
        record.update({

                "mtl_price_drop_cart_delta_indexes":
                list(row['mtl_price_drop_cart_delta_indexes']),
                "mtl_price_drop_cart_delta_indexes_update_at": current_ts
                })

    if row["mtl_price_drop_wishlist_delta_indexes"][0] != 'no_deltas':
        record.update({

                "mtl_price_drop_wishlist_delta_indexes":
                list(row['mtl_price_drop_wishlist_delta_indexes']),
                "mtl_price_drop_wishlist_delta_indexes_update_at": current_ts
                })

    return record if len(record) > 1 else None


def send_data_deltas_wrapper(batch_size=50):
    """
    Wrapper function that load data, build and add the delta records and send them async
    Args:
        batch_size (int, optional): Defaults to 50.
    """

    database = Variable.get('reverse_etl_database')

    logger.info('Loading table data_production_reverse_etl.braze_price_drop_deltas into '
                'DataFrame')
    df_agg_to_send = read_sql_table(
        table="braze_price_drop_deltas",
        database=database)

    total_records = get_data_to_send(df=df_agg_to_send,
                                     record_builder=build_record_deltas,
                                     batch_size=batch_size)

    asyncio.run(async_send_data(data_to_send=total_records))
