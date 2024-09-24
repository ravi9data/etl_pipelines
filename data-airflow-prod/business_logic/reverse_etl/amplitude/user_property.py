import asyncio
import json
import logging
import time

import pandas as pd
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from airflow.models import Variable
from awswrangler.s3 import read_parquet
from more_itertools import chunked

from plugins.s3_utils import get_s3_full_path
from plugins.utils.data_lake_helper import rewrite_data
from plugins.utils.pandas_helpers import prepare_df_for_s3
from plugins.utils.redshift import get_alchemy_connection
from plugins.utils.requests_utils import post_error_threshold_check
from plugins.utils.sql import read_sql_file

logger = logging.getLogger(__name__)

AMPLITUDE_IDENTIFY_API_KEY = 'amplitude_identify_api_key'
REDSHIFT_CONN_ID = 'redshift'
URL = 'https://api2.amplitude.com/identify'


def prepare_payload(df: pd.DataFrame, api_key: str) -> list:
    """Generates the list of payloads from the dataframe, where every row in
    the dataframe is a dict object. Output a list of nested dict objects, with
    the format like:

    {
        'api_key': 'xxxx',
        'identification': '[{
                            "user_id":"1234",
                            "user_properties":
                                            {
                                                "customer_label": "Lapsed",
                                                "Country": "Germany"
                                            }
                            }]'
    }
    """
    records = df.to_dict('records')
    records = [{'user_id': row.pop('user_id'), 'user_properties': row} for row in records]
    return [{'api_key': api_key,
            'identification': json.dumps([row], default=lambda x: None)
             } for row in records]


async def post_request(session, payload):
    await asyncio.sleep(0.5)
    async with session.post(url=URL, data=payload) as resp:
        return resp.status


async def send_many(payloads):
    tasks = []
    timeout = ClientTimeout(total=480)
    connector = TCPConnector(limit=100)
    async with ClientSession(timeout=timeout,
                             connector=connector,
                             raise_for_status=True) as session:
        for payload in payloads:
            tasks.append(asyncio.create_task(post_request(
                                                    session=session,
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
    config = Variable.get('amplitude_reverse_etl_settings', deserialize_json=True)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    glue_table = config['glue_table']
    interval_hour = config['interval_hour']

    logger.info('Reading user data from Redshift')
    query = read_sql_file(
        './dags/reverse_etl/amplitude/sql/extract_user_property.sql').format(
                                                                    interval_hour=interval_hour)
    user_data = pd.read_sql(
            sql=query,
            con=get_alchemy_connection(REDSHIFT_CONN_ID))
    logger.info(f'Number of records retrieved from Redshift: {user_data.shape[0]}')

    logger.info('Transforming user data dataframe for writing to S3')
    user_data, _ = prepare_df_for_s3(user_data)
    s3_writer = rewrite_data(
                    s3_destination_path=s3_destination_path,
                    glue_table_name=glue_table,
                    glue_database=glue_database,
                    final_data=user_data)

    return s3_writer['paths'][0]


def send_records_to_amplitude(**context):
    """
    The payloads are chunked to avoid overloading the API limit. We store the
    POST request status codes to calculate the % of failed requests.
    """
    config = Variable.get('amplitude_reverse_etl_settings', deserialize_json=True)
    api_key = Variable.get(AMPLITUDE_IDENTIFY_API_KEY)

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
    payloads = prepare_payload(df=user_df, api_key=api_key)

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
