import datetime
import logging

import pandas
from airflow.models import Variable
from awswrangler.s3 import delete_objects, list_objects, read_json

from plugins import s3_utils
from plugins.utils import aws, memory
from plugins.utils.data_lake_helper import rewrite_data
from plugins.utils.pandas_helpers import cast_df_as_str

logger = logging.getLogger(__name__)

COLUMNS_MAPPER = {
    '_metadata': 'metadata',
    'anonymousId': 'anonymous_id',
    'messageId': 'message_id',
    'projectId': 'project_id',
    'user_id': 'project_id',
    'originalTimestamp': 'original_timestamp',
    'receivedAt': 'received_at',
    'sentAt': 'sent_at',
    'event': 'event_name',
    'type': 'event_type'
}


def dataset_preparation(input_data: pandas.DataFrame, hour: datetime.datetime) -> pandas.DataFrame:
    input_data.rename(columns=COLUMNS_MAPPER, inplace=True)
    partitioned_data = s3_utils.add_partitions(input_data, hour, is_hour_based=True)
    memory.clean_from_memory(input_data)
    return cast_df_as_str(partitioned_data)


def delete_and_write(*, input_data: pandas.DataFrame, target_path: str,
                     target_partition_path: str, glue_db: str, glue_table: str):
    """
    Collect existing objects in the partition, write the data, and delete old existing objects.
    Doing so, we allow duplicates when writing, but no ghost reads will happen from readers.
    """
    existing_objects = list_objects(target_partition_path)
    logger.info(f'Existing objects in {target_partition_path} are {existing_objects}')

    # in case the data to rewrite is to heavy, adda chunker
    rewrite_data(s3_destination_path=target_path,
                 glue_database=glue_db,
                 glue_table_name=glue_table,
                 final_data=input_data)

    if existing_objects:
        logger.info(f'Deleting S3 objects {existing_objects} inside {target_partition_path}')
        delete_objects(existing_objects)
        logger.info(f'Deleted {existing_objects}')


def hour_rewriter(**context):
    """
    Read a source partition hour and re-write in the target destination
    """
    hour = pandas.to_datetime(context['params']['hour'])
    logger.info(f'Processing hour {hour}')
    config = Variable.get(context['params']['config_variable_name'], deserialize_json=True)
    glue_database = Variable.get('glue_db_segment')
    s3_bucket = config['s3_bucket']
    source_prefix = 'firehose/grover_js'
    destination_prefix = 'parquet/grover_js'
    glue_table_name = 'grover_js_raw_optimized'
    partition_name = s3_utils.get_partition_name(hour)

    full_source_path = f'{s3_bucket}/{source_prefix}/{partition_name}/'
    logger.info(f'Loading data from {full_source_path}')

    source_partition_objects = list_objects(path=f's3://{full_source_path}')
    logger.info(f'Objects in {full_source_path} are: {source_partition_objects}')

    if len(source_partition_objects) == 0:
        logger.info(f'Skipping execution, as {full_source_path} is empty')
        return

    raw_data = read_json(path=f's3://{full_source_path}',
                         lines=True,
                         boto3_session=aws.get_session(),
                         use_threads=False)

    logger.info(f'Source dataset size: {len(raw_data)}')
    if len(raw_data) > 0:
        final_data = dataset_preparation(raw_data, hour)
        full_target_path = f's3://{s3_bucket}/{destination_prefix}'

        delete_and_write(
            input_data=final_data,
            target_path=full_target_path,
            target_partition_path=f'{full_target_path}/{partition_name}/',
            glue_db=glue_database,
            glue_table=glue_table_name
        )
