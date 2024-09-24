import asyncio
import logging
import time

import backoff
import pandas as pd
from aiohttp import ClientError, ClientSession, ClientTimeout, TCPConnector
from airflow.models import Variable
from awswrangler.athena import read_sql_query as athena_read_sql_query

from business_logic.intercom.data_transformations import (get_page_count,
                                                          transform_dataframe,
                                                          transform_response)
from plugins.s3_utils import get_s3_full_path
from plugins.utils.data_lake_helper import rewrite_data
from plugins.utils.formatter import concatenate_list, range_chunker
from plugins.utils.memory import clean_from_memory
from plugins.utils.redshift import get_alchemy_connection
from plugins.utils.requests_utils import xratelimit_remaining
from plugins.utils.sql import read_sql_file

logger = logging.getLogger(__name__)

INTERCOM_API_KEY = 'intercom_api_key_v2.8'
REDSHIFT_CONN_ID = 'redshift'


@backoff.on_exception(backoff.expo, ClientError, max_tries=5, logger=logger)
async def get_request(session, url, headers, params, id_flag):
    await asyncio.sleep(0.5)
    async with session.get(url=url, headers=headers, params=params) as resp:
        if xratelimit_remaining(resp.headers["X-RateLimit-Remaining"], 20):
            logger.info('X-ratelimit reached, pausing for 5 seconds')
            time.sleep(5)
        if resp.status == 200:
            resp_json = await resp.json()
            if id_flag:
                return [transform_response(conv) for conv in resp_json['conversations']]
            else:
                return resp_json


async def fetch_many(iterator, url, headers, params, id_flag):
    """Start the aiohttp client session and pass the page numbers asynchronously.
    Then concatenates all the gathered reponses into one list

    Returns:
        List of all conversation entities for the entire page range
    """
    tasks = []
    timeout = ClientTimeout(total=1440)
    connector = TCPConnector(limit=100)
    async with ClientSession(timeout=timeout,
                             connector=connector,
                             raise_for_status=True) as session:
        for _iter in iterator:
            tasks.append(asyncio.ensure_future(get_request(session,
                                                           url.format(_iter),
                                                           headers,
                                                           params,
                                                           id_flag)))
        return await asyncio.gather(*tasks)


def asnyc_wrapper(function_callable, url, iterator, headers, params=None, id_flag=True):
    results = asyncio.run(function_callable(iterator=iterator,
                                            url=url,
                                            headers=headers,
                                            params=params,
                                            id_flag=id_flag))
    return results


def ingest_conversation_id(**context):
    """
    1. Extract the config settings from the Airflow Variable.
    2. Make the headers and params for making the GET requests.
    3. Retrieve the current total number of pages in Intercom.
    4. Split the pages into smaller ranges, and generate a batch_id for the overall run.
    6. For the chunked page ranges; trigger the Asyncio loop and write to S3/Glue.
    """
    task_instance = context['ti']
    batch_id = task_instance.xcom_pull(key='batch_id', task_ids='generate_batch_id')

    config = Variable.get('intercom_conversation_extract_settings', deserialize_json=True)
    header_token = Variable.get(INTERCOM_API_KEY)
    header_accept = config['header_accept']
    per_page = int(config.get('per_page', 150))
    params = (
        ('order', 'asc'),
        ('sort', 'created_at'),
        ('per_page', 150)
        )
    headers = dict(Authorization=header_token, Accept=header_accept)

    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix_id']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    glue_table_conv_id = config['glue_table_conv_id']

    total_pages = get_page_count(headers=headers, params=params, per_page=per_page)
    logger.info('Splitting total pages into range chunks')
    page_chunk = config['page_chunk']
    page_ranges = range_chunker(range(1, total_pages+1), int(page_chunk))

    for page_range in page_ranges:
        logger.info(f'Starting async request for page range: {page_range}')
        results = asnyc_wrapper(
                        function_callable=fetch_many,
                        url='https://api.intercom.io/conversations?&page={}',
                        iterator=page_range,
                        headers=headers,
                        params=params)

        logger.info('Concatenating response list')
        concat_list = concatenate_list(results)
        clean_from_memory(results)

        logger.info('Adding batch_id key to results dictionary')
        final_data = [dict(item, batch_id=batch_id) for item in concat_list]
        logger.info('Writing dataframe to S3')
        s3_writer = rewrite_data(
            s3_destination_path=s3_destination_path,
            glue_table_name=glue_table_conv_id,
            glue_database=glue_database,
            final_data=pd.DataFrame(final_data),
            partition_cols=['year', 'month', 'day'])
        clean_from_memory(final_data)

    for partition in s3_writer['partitions_values'].values():
        task_instance.xcom_push(key="year", value=partition[0])
        task_instance.xcom_push(key="month", value=partition[1])
        task_instance.xcom_push(key="day", value=partition[2])

    return s3_writer


def extract_raw_conversations(**context):

    task_instance = context['ti']
    config = Variable.get('intercom_conversation_extract_settings', deserialize_json=True)
    header_token = Variable.get(INTERCOM_API_KEY)
    header_accept = config['header_accept']
    headers = dict(Authorization=header_token, Accept=header_accept)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix_conv']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    glue_table_conv_raw = config['glue_table_conv_raw']
    interval_hour = int(config.get('interval_hour', 8))
    chunksize = int(config['chunksize'])
    sleep_seconds = int(config['sleep_seconds'])

    batch_id = task_instance.xcom_pull(key='batch_id', task_ids='generate_batch_id')
    year = task_instance.xcom_pull(key='year', task_ids='ingest_conversation_id')
    month = task_instance.xcom_pull(key='month', task_ids='ingest_conversation_id')
    day = task_instance.xcom_pull(key='day', task_ids='ingest_conversation_id')

    query = read_sql_file(
        './dags/intercom/sql/conversations/redshift_conversation_id.sql').format(
                                                            year=year,
                                                            month=month,
                                                            day=day,
                                                            batch_id=batch_id,
                                                            interval_hour=interval_hour)
    logger.info('Getting most recent conversation IDs from Redshift')
    df_iterator = pd.read_sql(sql=query,
                              con=get_alchemy_connection(REDSHIFT_CONN_ID),
                              chunksize=chunksize)

    s3_written_paths = []
    for counter, df in enumerate(df_iterator):
        logger.info(f'Processing chunk {counter+1}')
        results = asnyc_wrapper(
            function_callable=fetch_many,
            url='https://api.intercom.io/conversations/{}',
            iterator=df['id'].to_list(),
            headers=headers,
            id_flag=False)

        logging.info('Transforming response to dataframe')
        results_df = transform_dataframe(df=pd.DataFrame(results),
                                         columns=['contacts',
                                                  'teammates',
                                                  'custom_attributes',
                                                  'topics',
                                                  'tags',
                                                  'first_contact_reply',
                                                  'sla_applied',
                                                  'conversation_rating',
                                                  'statistics',
                                                  'attachments',
                                                  'author',
                                                  'conversation_parts'],
                                         batch_id_value=batch_id)
        s3_writer = rewrite_data(
            s3_destination_path=s3_destination_path,
            glue_table_name=glue_table_conv_raw,
            glue_database=glue_database,
            final_data=results_df,
            partition_cols=['year', 'month', 'day'])
        clean_from_memory(results_df)
        s3_written_paths.extend(s3_writer['paths'])

        time.sleep(sleep_seconds)
        logger.info(f'Pausing for {sleep_seconds} seconds before the next extraction')

    return s3_written_paths


def denest_conversation_parts(**context):

    task_instance = context['ti']
    batch_id = task_instance.xcom_pull(key='batch_id', task_ids='generate_batch_id')
    year = task_instance.xcom_pull(key='year', task_ids='ingest_conversation_id')
    month = task_instance.xcom_pull(key='month', task_ids='ingest_conversation_id')
    day = task_instance.xcom_pull(key='day', task_ids='ingest_conversation_id')

    config = Variable.get('intercom_conversation_extract_settings', deserialize_json=True)
    workgroup = Variable.get('athena_workgroup', default_var='deng-applications')
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix_conv_parts']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    glue_table_conv_raw = config['glue_table_conv_raw']
    glue_table_conv_parts = config['glue_table_conv_parts']

    query = read_sql_file(
        './dags/intercom/sql/conversations/athena_conversation_parts.sql')
    logger.info('Getting denested conversation_parts from Athena')
    df_conv_parts = athena_read_sql_query(
                    sql=query,
                    database=glue_database,
                    params={
                            "table_conversations_extract": f"{glue_table_conv_raw}",
                            "batch_id": f"'{batch_id}'",
                            "year": f"{year}",
                            "month": f"'{month}'",
                            "day": f"'{day}'"
                            },
                    keep_files=False,
                    workgroup=workgroup
                    )
    logger.info('Stored denested conversation_parts stored in a dataframe')

    s3_written_paths = []
    logger.info('Writing denested conversation_parts to S3')
    s3_writer = rewrite_data(
            s3_destination_path=s3_destination_path,
            glue_table_name=glue_table_conv_parts,
            glue_database=glue_database,
            final_data=df_conv_parts,
            partition_cols=['year', 'month', 'day'])

    s3_written_paths.extend(s3_writer['paths'])

    return s3_written_paths
