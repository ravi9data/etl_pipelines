import asyncio
import logging
import time

import pandas as pd
from airflow.models import Variable
from awswrangler.athena import read_sql_query as athena_read_sql_query
from awswrangler.s3 import read_parquet
from more_itertools import chunked

from plugins.s3_utils import get_s3_full_path
from plugins.utils.braze.reverse_etl import prepare_payload, send_many
from plugins.utils.data_lake_helper import (rewrite_data,
                                            s3_writer_without_partitions)
from plugins.utils.pandas_helpers import (prepare_df_for_s3,
                                          transform_nulls_to_empty_string)
from plugins.utils.redshift import get_alchemy_connection
from plugins.utils.requests_utils import post_error_threshold_check
from plugins.utils.sql import read_sql_file

logger = logging.getLogger(__name__)

BRAZE_API_KEY_VAR_NAME = 'braze_api_key'
BRAZE_URL = 'braze_url'
REDSHIFT_CONN_ID = 'redshift'


def export_redshift_data_to_s3(**context):
    """
    Reads the customer attributes from Redshift and loads this into a DataFrame. Due to
    the DAG schedule frequency, it is possible to have zero records, so the function exits
    if this is the case. Otherwise, the data is written to a staging location to S3 in a
    single file, which is always overwritten.
    """
    config = Variable.get('braze_custom_attributes_export_settings', deserialize_json=True)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix_stage']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    glue_table = config['glue_table_stage']
    interval_minutes = config['interval_minutes']

    logger.info('Reading user data from Redshift')
    query = read_sql_file(
        './dags/reverse_etl/braze/custom_attributes/sql/redshift_staging_attributes.sql').format(
                                                            interval_minutes=interval_minutes)
    user_data = pd.read_sql(
            sql=query,
            con=get_alchemy_connection(REDSHIFT_CONN_ID))

    if user_data.shape[0] == 0:
        logger.info('No staging records present during this run, exiting DAG ...')
        return False
    else:
        logger.info(f'Number of records retrieved from Redshift: {user_data.shape[0]}')

    logger.info('Writing staging records to S3')
    user_data = transform_nulls_to_empty_string(input_df=user_data)
    s3_writer = s3_writer_without_partitions(
                        df=user_data,
                        s3_destination_path=s3_destination_path,
                        glue_table_name=glue_table,
                        glue_database=glue_database,
                        mode='overwrite')

    task_instance = context['ti']
    task_instance.xcom_push(key='s3_stage_path', value=s3_writer['paths'][0])

    return True


def get_delta_records(**context):
    """
    Compares the staging table vs the historical table via an Athena query, to get check
    for possible delta records. If the number of records are zero, the function exits.
    Otherwise, the delta records are appended to the historical table, and the written
    path in S3 is pushed to XCOM for use in downstream tasks.
    """
    config = Variable.get('braze_custom_attributes_export_settings', deserialize_json=True)
    workgroup = Variable.get('athena_workgroup', default_var='deng-applications')
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix_hist']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    glue_table_stage = config['glue_table_stage']
    glue_table_hist = config['glue_table_hist']

    logger.info('Comparing stage records against historical records in Athena')
    query = read_sql_file('./dags/reverse_etl/braze/custom_attributes/sql/athena_delta_records.sql')
    delta_data = athena_read_sql_query(
                    sql=query,
                    database=glue_database,
                    params={
                            "braze_hist_table": glue_table_hist,
                            "braze_stage_table": glue_table_stage
                            },
                    keep_files=False,
                    workgroup=workgroup)

    if delta_data.shape[0] == 0:
        logger.info('No delta records present during this run, exiting DAG ...')
        return False
    else:
        logger.info(f'Number of delta records retrieved from Athena: {delta_data.shape[0]}')

    logger.info('Preparing delta records dataframe for writing to S3')
    transformed_data = transform_nulls_to_empty_string(input_df=delta_data)
    transformed_data, _ = prepare_df_for_s3(df=transformed_data)

    logger.info('Appending delta records to S3')
    s3_writer = rewrite_data(
                    s3_destination_path=s3_destination_path,
                    glue_table_name=glue_table_hist,
                    glue_database=glue_database,
                    final_data=transformed_data,
                    partition_cols=['year', 'month', 'day', 'hour'])

    task_instance = context['ti']
    task_instance.xcom_push(key='s3_delta_path', value=s3_writer['paths'][0])

    return True


def send_records_to_braze(**context):
    """
    Pulls the delta records file path from XCOM, and transforms the dataframe into
    the payload format as expected by Braze. The payloads are chunked to avoid exceeding
    the Braze API limit. The chunks are asynchronously sent, and the HTTP return response
    codes are kept to keep track of any failures during the send process. If the threshold
    value is exceeded, the function throws an exception and exits.
    """
    config = Variable.get('braze_custom_attributes_export_settings', deserialize_json=True)
    chunksize = int(config['chunksize'])
    sleep_seconds = int(config['sleep_seconds'])
    post_error_threshold = float(config['post_error_threshold'])

    s3_path = context['ti'].xcom_pull(key='s3_delta_path', task_ids='get_delta_records')
    logger.info(f'Reading data from: {s3_path}')
    delta_data = read_parquet(path=s3_path, ignore_index=True)

    drop_columns = ['batch_id', 'extracted_at']
    logger.info(f'Dropping these columns from the dataframe: {drop_columns}')
    payload_data = delta_data.drop(columns=drop_columns)

    logger.info('Transforming payload dataframe into list of payloads')
    payloads = prepare_payload(input_df=payload_data)

    logger.info(f'Chunking payload list into chunks of {chunksize} elements')
    chunks = chunked(iterable=payloads, n=chunksize)

    logger.info('Starting async POST requests to Braze')
    post_results = []
    headers = {
                'Authorization': "Bearer " + Variable.get(BRAZE_API_KEY_VAR_NAME),
                'Content-Type': 'application/json',
                'Accept': 'application/json'
              }

    for counter, payloads in enumerate(chunks):
        logger.info(f'Async POST request for chunk # :{counter}')
        post_result = asyncio.run(send_many(headers=headers, payloads=payloads))
        post_results.extend(post_result)

        logger.info(f'Pausing for {sleep_seconds} seconds before the next POST loop')
        time.sleep(sleep_seconds)

    logger.info('Checking for POST errors')
    post_error_threshold_check(post_results, post_error_threshold)
