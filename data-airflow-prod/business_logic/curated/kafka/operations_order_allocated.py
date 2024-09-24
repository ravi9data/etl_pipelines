import datetime
import gc
import json
import logging

import pandas as pd
from airflow.models import Variable
from awswrangler.s3 import (delete_objects, list_objects, read_parquet,
                            to_parquet)

from plugins import s3_utils
from plugins.utils.pandas_helpers import cast_df_as_str

logger = logging.getLogger(__name__)


def add_new_columns(raw_data):
    new_df = raw_data.copy()
    new_df['processed_at'] = datetime.datetime.now()
    new_df['allocations'] = new_df['payload'].apply(lambda x: json.loads(x)['allocations'])
    new_df['user_id'] = new_df['payload'].apply(
        lambda x: json.loads(x)['user_id'] if 'user_id' in x else None).fillna(0)
    new_df['user_id'] = new_df['user_id'].astype("Int64")
    new_df['order_number'] = new_df['payload'].apply(lambda x: json.loads(x)['order_number'])
    new_df = new_df.explode(column='allocations', ignore_index=False)
    new_df.rename(columns={'allocations': 'allocation_item'}, inplace=True)
    del new_df['payload']
    clean_df = cast_df_as_str(new_df)
    return clean_df


def rewrite_data(s3_destination_path, glue_table_name, glue_database, final_data):
    # delete object using the country prefix with the date
    logger.info(f's3 destination Prefix {s3_destination_path}')
    # write with append parquet files
    logger.info(f'Destination path to write to: {str(s3_destination_path)}')
    result = to_parquet(
        df=final_data,
        path=s3_destination_path,
        use_threads=False,
        index=False,
        dataset=True,
        database=glue_database,
        table=glue_table_name,
        schema_evolution=True,
        sanitize_columns=True,
        mode="append",
        partition_cols=['year', 'month', 'day', 'hour'],
        max_rows_by_file=15000
    )
    logger.info(f'Dataset written: {str(result)}')
    return result['paths']


def operations_order_allocated_hour_rewriter(**context):
    glue_table_name = context['params']['glue_table_name']
    config_variable_name = context['params']['config_variable_name']
    hour = pd.to_datetime(context['params']['hour'])
    config = Variable.get(config_variable_name, deserialize_json=True)

    logger.info(f'Processing hour: {hour}')
    # s3 configs
    s3_source_bucket = config['s3_source_bucket']
    s3_source_prefix = config['s3_source_prefix']
    glue_database = config['glue_database']
    s3_source_prefix = s3_utils.prefix_builder(s3_source_prefix, hour)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_destination_prefix = config['s3_destination_prefix']
    s3_destination_path = s3_utils.get_s3_full_path(s3_destination_bucket, s3_destination_prefix)

    source_list_objects = list_objects(
        s3_utils.get_s3_full_path(s3_source_bucket, s3_source_prefix))
    # Prepare the list of files we need to delete
    full_target_path = s3_utils.get_target_s3_path(s3_destination_path, hour)
    logger.info(f'Collecting exiting objects in {full_target_path}')
    existing_objects = list_objects(full_target_path)
    logger.info(f'Size of source list objects {len(source_list_objects)}')
    if len(source_list_objects) > 0:
        raw_data = read_parquet(
            source_list_objects,
            columns=['event_name', 'payload', 'consumed_at'],
            map_types=True,
            ignore_index=True,
            pyarrow_additional_kwargs={
                "coerce_int96_timestamp_unit": "ms",
                "timestamp_as_object": False
            })
        # de-nesting of allocations
        logger.info(raw_data.columns)
        nested_data = add_new_columns(raw_data)
        del raw_data
        gc.collect()
        final_data = s3_utils.add_partitions(nested_data, hour, is_hour_based=True)
        del nested_data
        gc.collect()
        rewrite_data(
            s3_destination_path=s3_destination_path,
            glue_table_name=glue_table_name,
            glue_database=glue_database,
            final_data=final_data)
        if existing_objects:
            logger.info(f'Deleting old S3 objects {existing_objects} inside {full_target_path}')
            delete_objects(existing_objects)
            logger.info(f'Deleted {existing_objects}')
    else:
        logger.info(f'{source_list_objects} is empty!')
