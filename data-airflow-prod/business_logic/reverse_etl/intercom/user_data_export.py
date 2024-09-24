import asyncio
import json
import logging
import time

import backoff
import pandas as pd
from aiohttp import ClientError, ClientSession, ClientTimeout, TCPConnector
from airflow.models import Variable
from awswrangler.s3 import read_parquet
from more_itertools import chunked

from plugins.s3_utils import get_s3_full_path
from plugins.utils.data_lake_helper import rewrite_data
from plugins.utils.pandas_helpers import (empty_cols_to_string_type,
                                          prepare_df_for_s3,
                                          remove_trailing_zeros)
from plugins.utils.redshift import get_alchemy_connection
from plugins.utils.requests_utils import (post_error_threshold_check,
                                          xratelimit_remaining)
from plugins.utils.sql import read_sql_file

logger = logging.getLogger(__name__)

INTERCOM_EXPORT_API_KEY = 'intercom_export_api_key'
REDSHIFT_CONN_ID = 'redshift'
URL = 'https://api.intercom.io/users'


def prepare_payload(df: pd.DataFrame) -> list:
    """Generates the list of payloads from the dataframe, where every row in
    the dataframe is a dict object. Output a list of nested dict objects, with
    the format like:

        {
            "user_id":"12345",
            "custom_attributes":{"xxx":"abc", "yyy":"def"}
        }

    """
    records = df.to_dict('records')
    return [{'user_id': row.pop('user_id'), 'custom_attributes': row} for row in records]


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Firstly adds the two columns as given by CS team:

    "redshift_synced_at": the timestamp
    "sync_test_number": Defaults to '9'

    Then transforms the dataframe with the util functions to make it ready for
    writing to S3.
    """
    df['redshift_synced_at'] = int(time.time())
    df['sync_test_number'] = 9
    df.fillna('', inplace=True)
    df = empty_cols_to_string_type(df, all_columns_as_string=True)
    for col in ['subscription_limit',
                'last_abandoned_cart_at',
                'next_min_cancellation_at']:
        df = remove_trailing_zeros(df, col)
    df, _ = prepare_df_for_s3(df)

    return df


@backoff.on_exception(backoff.expo, ClientError, max_tries=6, logger=logger)
async def post_request(session, headers, payload):
    await asyncio.sleep(0.5)
    async with session.post(url=URL,
                            headers=headers,
                            data=json.dumps(payload)) as resp:
        if xratelimit_remaining(resp.headers["X-RateLimit-Remaining"], 20):
            logger.info('X-ratelimit reached, pausing for 5 seconds')
            time.sleep(5)
        return resp.status


async def send_many(payloads):
    tasks = []
    headers = {
                'Authorization': Variable.get(INTERCOM_EXPORT_API_KEY),
                'Content-Type': 'application/json',
                'Accept': 'application/json'
              }
    timeout = ClientTimeout(total=480)
    connector = TCPConnector(limit=100)
    async with ClientSession(timeout=timeout,
                             connector=connector,
                             raise_for_status=True) as session:
        for payload in payloads:
            tasks.append(asyncio.create_task(post_request(
                                                    session=session,
                                                    headers=headers,
                                                    payload=payload)))

        return await asyncio.gather(*tasks)


def export_redshift_data_to_s3(**context):
    """
    Retrieves the user data from Redshift for the specified time interval, and
    loads the query results into a dataframe. The dataframe is transformed via the
    utils functions, to append the batch_id, timestamp, and partition columns.

    The data is written to S3 so that we have the records of data being sent to
    Intercom API for the specific time period and batch.

    Returns: S3 parquet file path
    """
    config = Variable.get('intercom_reverse_etl_settings', deserialize_json=True)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    glue_table = config['glue_table']
    interval_hour = config['interval_hour']

    logger.info('Reading user data from Redshift')
    query = read_sql_file(
        './dags/reverse_etl/intercom/sql/extract_user_data.sql').format(
                                                                    interval_hour=interval_hour)
    user_data = pd.read_sql(
            sql=query,
            con=get_alchemy_connection(REDSHIFT_CONN_ID))
    logger.info(f'Number of records retrieved from Redshift: {user_data.shape[0]}')

    logger.info('Transforming user data dataframe for writing to S3')
    user_data = transform_dataframe(user_data)
    s3_writer = rewrite_data(
                    s3_destination_path=s3_destination_path,
                    glue_table_name=glue_table,
                    glue_database=glue_database,
                    final_data=user_data)

    return s3_writer['paths'][0]


def send_records_to_intercom(**context):
    """
    This is
    transformed into the payload for the POST requests, following the requirements
    given by CS team. The payloads are chunked to avoid overloading the Intercom API
    limit. We store the POST request status codes to calculate the % of failed requests.

    The second part converts the full list of payload into a dataframe, and adds the
    batch_id, extracted_at, and partition columns to it. This is then written to S3, in
    append mode. The history of sent records can be found in the associated Glue table.
    """
    config = Variable.get('intercom_reverse_etl_settings', deserialize_json=True)
    chunksize = int(config['chunksize'])
    sleep_seconds = int(config['sleep_seconds'])
    post_error_threshold = float(config['post_error_threshold'])

    s3_path = context['ti'].xcom_pull(key='return_value', task_ids='export_redshift_data_to_s3')
    logger.info(f'Reading data from: {s3_path}')
    user_df = read_parquet(path=s3_path, ignore_index=True)

    drop_columns = ['batch_id', 'extracted_at']
    logger.info(f'Dropping these columns from the dataframe: {drop_columns}')
    user_df = user_df.drop(columns=drop_columns)

    logger.info('Preparing payload list')
    payloads = prepare_payload(user_df)

    logger.info(f'Chunking payload list into chunks of {chunksize} elements')
    chunks = chunked(iterable=payloads, n=chunksize)

    post_results = []
    for counter, payloads in enumerate(chunks):
        logger.info(f'Starting async POST requests for chunk # :{counter}')
        post_result = asyncio.run(send_many(payloads))
        post_results.extend(post_result)

        logger.info(f'Pausing for {sleep_seconds} seconds before the next POST loop')
        time.sleep(sleep_seconds)

    logger.info('Checking for POST errors')
    post_error_threshold_check(post_results, post_error_threshold)
