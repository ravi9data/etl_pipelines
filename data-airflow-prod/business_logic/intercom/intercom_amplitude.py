import gc
import logging
import os.path

import awswrangler as wr
import pandas
import toolz
from airflow.models import Variable

from plugins import s3_utils
from plugins.utils import aws
from plugins.utils.pandas_helpers import cast_df_as_str

logger = logging.getLogger(__name__)

EXPORTS_PREFIX = 'exports/297828/297828'
BATCH_OBJECTS_SIZE = 4


def prefix_builder(input_datetime):
    _date = input_datetime.strftime('%Y-%m-%d')
    _hour = int(input_datetime.strftime('%H'))
    # hash is used to terminate the hour export in the path
    return f'{EXPORTS_PREFIX}_{_date}_{_hour}#'


def amplitude_rewriter_partial(hour, batch, s3_destination_path,
                               glue_database, glue_table_name):
    """
    Receive a batch of s3 keys, and take care of reading them, and re-write to S3 as parquet.
    """
    logger.info(f'Processing {batch}')
    session = aws.get_session()
    raw_data = wr.s3.read_json(path=list(batch),
                               lines=True,
                               boto3_session=session,
                               use_threads=False)
    logger.info(f'Records loaded: {len(raw_data)}')
    logger.info('Starting to clean dataset')
    clean_data = cast_df_as_str(raw_data)
    logger.info('Data cleaned')
    del raw_data
    gc.collect()
    logger.debug('Starting to add partitions')
    final_data = s3_utils.add_partitions(clean_data, hour, is_hour_based=True)
    logger.debug('Partitions added')
    del clean_data
    gc.collect()
    logger.info('Writing data to S3')
    result = wr.s3.to_parquet(
        df=final_data,
        boto3_session=session,
        path=s3_destination_path,
        use_threads=False,
        index=False,
        dataset=True,
        database=glue_database,
        table=glue_table_name,
        schema_evolution=True,
        mode="append",
        partition_cols=['year', 'month', 'day', 'hour'],
        max_rows_by_file=15000
    )
    logger.info(f'Dataset written: {str(result)}')
    return result['paths']


def get_prefix_to_delete(hour, s3_destination_path):
    partition = s3_utils.get_partition_name(hour)
    return os.path.join(s3_destination_path, partition)


def amplitude_hour_rewriter(**context):
    """
    Given one hour, it read the correspondent files from Amplitude exports
    and re-write them to S3 in parquet format, registering the dataset to a glue table.
    If the process it's called multiple times, the function take care of deleting and re-inserting.
    """
    glue_table_name = context['params']['glue_table_name']
    config_variable_name = context['params']['config_variable_name']
    hour = pandas.to_datetime(context['params']['hour'])
    config = Variable.get(config_variable_name, deserialize_json=True)

    logger.info(f'Processing hour: {hour}')

    # s3 configs
    s3_source_bucket = config['s3_source_bucket']
    s3_source_prefix = prefix_builder(hour)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_destination_extra_prefix = context['params'].get('s3_destination_extra_prefix', '')
    s3_destination_prefix = os.path.join(s3_destination_extra_prefix, glue_table_name)
    s3_destination_path = s3_utils.get_s3_full_path(s3_destination_bucket, s3_destination_prefix)

    s3_client = s3_utils.get_s3_client()
    source_objects = s3_utils.get_s3_paginator(s3_client, s3_source_bucket, s3_source_prefix)
    logger.info(f'Total source objects: {len(source_objects)}')
    if source_objects:
        filtered_objects = [s3_utils.get_s3_full_path(s3_source_bucket, o['Key'])
                            for o in source_objects
                            if o['Key'].endswith('json.gz')]
        logger.info(filtered_objects)
        s3_written = []
        prefix_to_delete = get_prefix_to_delete(hour, s3_destination_prefix)
        s3_utils.delete_objects_by_prefix(s3_client, s3_destination_bucket, prefix_to_delete)
        logger.info(f'Delete everything inside {prefix_to_delete} inside {s3_destination_bucket}')

        batches = list(toolz.partition_all(BATCH_OBJECTS_SIZE, filtered_objects))

        # process max 5 files at the time to avoid OoM
        for batch in batches:
            result = amplitude_rewriter_partial(hour, batch, s3_destination_path,
                                                config['glue_database'], glue_table_name)
            s3_written.extend(result)
        return s3_written
    return []
