
import logging

import pandas as pd
from airflow.models import Variable
from awswrangler.athena import read_sql_query as athena_read_sql_query
from awswrangler.s3 import read_json
from more_itertools import chunked

from business_logic.intercom.commons import get_filenames_list
from plugins.utils.data_lake_helper import rewrite_data
from plugins.utils.memory import clean_from_memory
from plugins.utils.pandas_helpers import (add_inserted_at_columns,
                                          add_partition_columns,
                                          empty_cols_to_string_type,
                                          global_df_json_encoder,
                                          insert_batch_id,
                                          remove_trailing_zeros)
from plugins.utils.sql import read_sql_file

logger = logging.getLogger(__name__)


DAG_CONFIG = 'intercom_conversations_export_config'
REDSHIFT_CONN_ID = 'redshift'

expected_columns = ['type',
                    'id',
                    'title',
                    'created_at',
                    'updated_at',
                    'waiting_since',
                    'snoozed_until',
                    'open',
                    'state',
                    'read',
                    'priority',
                    'admin_assignee_id',
                    'team_assignee_id',
                    'tags',
                    'conversation_rating',
                    'source',
                    'contacts',
                    'teammates',
                    'custom_attributes',
                    'first_contact_reply',
                    'sla_applied',
                    'statistics',
                    'conversation_parts']


def match_expected_columns(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Column list is from the API documentation. If some are missing
    in the export, adds them and fill value with NaN.
    """
    df = df.reindex(columns=cols)
    return df


def split_source_column(df: pd.DataFrame) -> pd.DataFrame:
    """Explode the "source" json object to sub-objects due to column length.
    Drop the orignal column then concat the newly formed columns"""
    add_df = pd.json_normalize(df['source'], max_level=0)
    keep_list = ['attachments',
                 'author',
                 'body',
                 'delivered_as',
                 'id',
                 'subject',
                 'url',
                 'redacted']
    add_df = add_df.reindex(columns=keep_list)
    add_df.rename({'id': 'source_id'}, axis=1, inplace=True)
    df.drop('source', axis=1, inplace=True)
    df.reset_index(inplace=True, drop=True)
    df = pd.concat([df, add_df], axis=1)
    return df


def split_conversations_parts(df: pd.DataFrame) -> pd.DataFrame:
    """Explode the "conversation_parts" json object to sub-objects
    due to column length. Drop the orignal column then concat the newly
    formed columns"""
    add_df = pd.json_normalize(df['conversation_parts'])
    df.drop('conversation_parts', axis=1, inplace=True)
    df.reset_index(inplace=True, drop=True)
    add_df = add_df[[c for c in add_df.columns if c not in df.columns]]
    df = pd.concat([df, add_df], axis=1)
    return df


def transform_dataframe(df: pd.DataFrame, batch_id_value: str) -> pd.DataFrame:
    """Adds extracted_at and batch_id columns, and partitions using the pandas_helpers
    plugin utility.
    """
    logger.info('Filling in missing columns')
    df = match_expected_columns(df=df, cols=expected_columns)
    logger.info("Transforming dataframe")
    df = split_source_column(df=df)
    df = split_conversations_parts(df=df)
    df = global_df_json_encoder(input_df=df)
    df = empty_cols_to_string_type(df=df, all_columns_as_string=True)
    logger.info("Removing trailing zeros")
    for col in ['waiting_since',
                'snoozed_until',
                'admin_assignee_id',
                'team_assignee_id']:
        df = remove_trailing_zeros(df=df, col=col)
    logger.info('Inserting `extracted_at` and partition columns')
    df, _ = insert_batch_id(df=df, batch_id_col='batch_id', batch_id_value=batch_id_value)
    df = add_inserted_at_columns(df=df, column_name='extracted_at')
    df = add_partition_columns(df=df,
                               col='extracted_at',
                               partition_columns_to_add=['year', 'month', 'day'])
    return df


def transform_raw_conversations(**context):

    task_instance = context['ti']
    config = Variable.get(DAG_CONFIG, deserialize_json=True)
    s3_csv_path = config['s3_csv_path']
    s3_curated_path = config['s3_curated_path']
    glue_database = config['glue_database']
    glue_table = config['glue_table_conv']
    chunksize = int(config['chunksize'])
    batch_id = task_instance.xcom_pull(key='batch_id', task_ids='generate_batch_id')

    s3_written_paths = []
    all_files = get_filenames_list(s3_path=s3_csv_path)
    chunks = chunked(all_files, chunksize)
    for counter, f in enumerate(chunks):
        logger.info(f'Processing chunk {counter}')
        input_df = read_json(path=f, convert_dates=False)
        results_df = transform_dataframe(df=input_df, batch_id_value=batch_id)
        s3_writer = rewrite_data(s3_destination_path=s3_curated_path,
                                 glue_table_name=glue_table,
                                 glue_database=glue_database,
                                 final_data=results_df,
                                 partition_cols=['year', 'month', 'day'])
        clean_from_memory(results_df)
        s3_written_paths.extend(s3_writer['paths'])

    year = s3_written_paths[0].split("year=")[1].split("/")[0]
    month = s3_written_paths[0].split("month=")[1].split("/")[0]
    day = s3_written_paths[0].split("day=")[1].split("/")[0]

    logger.info('Pushing partition values to XCOM')
    task_instance.xcom_push(key="year", value=year)
    task_instance.xcom_push(key="month", value=month)
    task_instance.xcom_push(key="day", value=day)

    return s3_written_paths


def denest_conversations_parts(**context):

    task_instance = context['ti']
    batch_id = task_instance.xcom_pull(key='batch_id', task_ids='generate_batch_id')
    year = task_instance.xcom_pull(key='year', task_ids='transform_raw_conversations')
    month = task_instance.xcom_pull(key='month', task_ids='transform_raw_conversations')
    day = task_instance.xcom_pull(key='day', task_ids='transform_raw_conversations')

    config = Variable.get(DAG_CONFIG, deserialize_json=True)
    workgroup = Variable.get('athena_workgroup', default_var='deng-applications')
    s3_destination_path = config['s3_conv_parts_path']
    glue_database = config['glue_database']
    glue_table_conv = config['glue_table_conv']
    glue_table_conv_parts = config['glue_table_conv_parts']

    query = read_sql_file(
        './dags/intercom/sql/conversations_export/athena_conversations_parts.sql')
    logger.info('Getting denested conversation_parts from Athena')
    df_conv_parts = athena_read_sql_query(
                    sql=query,
                    database=glue_database,
                    params={
                            "table_conversations": f"{glue_table_conv}",
                            "batch_id": f"'{batch_id}'",
                            "year": f"{year}",
                            "month": f"'{month}'",
                            "day": f"'{day}'"
                            },
                    keep_files=False,
                    workgroup=workgroup
                    )
    logger.info('Stored denested conversations parts stored in a dataframe')

    s3_written_paths = []
    logger.info('Writing denested conversations parts to S3')
    s3_writer = rewrite_data(
            s3_destination_path=s3_destination_path,
            glue_table_name=glue_table_conv_parts,
            glue_database=glue_database,
            final_data=df_conv_parts,
            partition_cols=['year', 'month', 'day'],
            max_rows_by_file=50000)

    s3_written_paths.extend(s3_writer['paths'])

    return s3_written_paths
