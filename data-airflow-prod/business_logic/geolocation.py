import datetime
import gc
import logging
import os

import pandas as pd
from airflow.models import Variable
from awswrangler.s3 import delete_objects, list_objects, to_parquet
from more_itertools import chunked

from plugins import s3_utils

logger = logging.getLogger(__name__)


def get_partition_name(partition_date) -> str:
    """
    Given a datetime.datetime object it return a partition based
    on Hive convention, using named partitions and with specific country code.
    The default partitions are year, month, day.
    """
    _year = str(partition_date.year)
    _month = "{:02d}".format(partition_date.month)
    _day = "{:02d}".format(partition_date.day)
    return f"year={_year}/month={_month}/day={_day}"


def get_target_s3_path(s3_destination_prefix, date_now):
    partition = get_partition_name(date_now)
    return os.path.join(s3_destination_prefix, partition)


def rewrite_data(index_chunks, s3_destination_path, glue_table_name, glue_database, data):
    logger.info(f'S3 destination Prefix {s3_destination_path}')
    results = []
    for index_i in index_chunks:
        result = to_parquet(
            df=data.loc[index_i],
            path=s3_destination_path,
            use_threads=False,
            index=False,
            dataset=True,
            database=glue_database,
            table=glue_table_name,
            schema_evolution=True,
            mode="append",
            partition_cols=['year', 'month', 'day']
        )
        s3_written_objects = result.get('paths')
        logger.info(f'Dataset written: {str(s3_written_objects)}')
        results.extend(result.get('paths'))
    return results


def geolocation_writer(**context):
    """
    Retrieve data from URL, for all countries geolocation
    Write it in a parquet format in s3 bucket
    """
    config = Variable.get('geolocation_config', deserialize_json=True)
    s3_destination_bucket = config['s3_bucket']
    s3_destination_prefix = context['params']['s3_bucket_prefix']
    glue_table_name = context['params']['glue_table_name']
    s3_destination_path = s3_utils.get_s3_full_path(s3_destination_bucket, s3_destination_prefix)
    glue_database = config['glue_database']
    url = context['params']['url']
    chunk_size = context['params']['chunk_size']

    logger.info(f'Extracting data from {url}')
    column_names = context['params']['column_names']
    raw_data = pd.read_csv(
        url,
        compression='zip',
        names=column_names,
        header=None,
        dtype='str',
        sep='\t'
    )
    logger.info(f'Records loaded: {len(raw_data)}')
    if len(raw_data) == 0:
        raise IndexError('Extracted data is empty')

    logger.debug('Starting to add partitions')
    date_now = datetime.datetime.utcnow()
    final_data = s3_utils.add_partitions(raw_data, date_now, is_hour_based=False)
    del raw_data
    gc.collect()
    # Prepare the list of files we need to delete
    full_target_path = get_target_s3_path(s3_destination_path, date_now)
    logger.info(f'Collecting exiting objects in {full_target_path}')
    existing_objects = list_objects(full_target_path)
    logger.info('Writing data to S3')
    index_chunks = chunked(final_data.index, chunk_size)
    s3_written_objects = rewrite_data(
        index_chunks=index_chunks,
        s3_destination_path=s3_destination_path,
        glue_table_name=glue_table_name,
        glue_database=glue_database,
        data=final_data)
    logger.info(f'Dataset written in {len(s3_written_objects)} objects')
    # after inserting we delete files if exists in the prefix
    # We do the delete after, to make sure we have data in case there is any transaction
    # then people see duplicates and not missing data
    if existing_objects:
        logger.info(f'Deleting old S3 objects {existing_objects} inside {full_target_path}')
        delete_objects(existing_objects)
        logger.info(f'Deleted {existing_objects}')
