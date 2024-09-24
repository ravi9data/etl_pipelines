import logging
import os
from datetime import date

import pandas as pd
from airflow.models import Variable
from awswrangler.s3 import list_objects, read_excel, read_json

from plugins.utils.data_lake_helper import rewrite_data
from plugins.utils.memory import clean_from_memory
from plugins.utils.pandas_helpers import global_df_json_encoder
from plugins.utils.recommerce import (extract_date_from_filename,
                                      get_delta_files_remote_server_and_s3,
                                      transform_dataframe,
                                      write_delta_filelist_s3)

logger = logging.getLogger(__name__)


def get_source_files_delta(dag_config: Variable,
                           source_sftp_conn_id: str,
                           **context):
    """Compares the files in the remote sftp server against the available files in s3.
    If there are no delta, then exits and skips downstream tasks. Else, writes the list
    of delta filenames to one file in s3.

    Args:
        source_sftp_conn_id (str): sftp server connection string, from Airflow connections.
        remote_path (str): Remote sftp server directory for input files.
        file_filter (str): Filter out unexpected filenames from remote file list.

    The output csv files consists of a list of delta filenames to be downloaded. This file
    is overwritten each run.

    Returns:
        bool: The downstream tasks ID
    """
    config = Variable.get(dag_config, deserialize_json=True)
    s3_existing_files_path = config.get('s3_existing_files_path')
    remote_server_path = config.get('remote_server_path')
    source_files_to_download_csv_path = config.get('source_files_to_download_csv_path')
    today = str(date.today())
    year_day_extract = today[:7]

    logger.info(f'Comparing existing files in {s3_existing_files_path} against'
                f' files available in sftp server "{remote_server_path}".')
    delta_filenames = get_delta_files_remote_server_and_s3(s3_path=s3_existing_files_path,
                                                           sftp_conn_id=source_sftp_conn_id,
                                                           file_filter=year_day_extract,
                                                           remote_server_path=remote_server_path)
    if not delta_filenames:
        logger.info('No delta files present this run, skipping tasks.')
        return 'slack_message_no_delta'
    else:
        logger.info(f'We have {len(delta_filenames)} file(s) to download for this run.')
        s3_written_path = write_delta_filelist_s3(delta_files=delta_filenames,
                                                  remote_path=remote_server_path,
                                                  s3_path=source_files_to_download_csv_path)
        logger.info(f'Delta filenames written to "{s3_written_path}"')
        return 'download_delta_source_files_to_s3'


def transform_raw_source_files(s3_path: str,
                               object_suffix: str,
                               skiprows: int = None,
                               sep: str = '_') -> list:
    """Read all the files in the specified path into a list of
    dataframes. The output list will be concatened by the parent function.

    Args:
        s3_path (str): Path of files
        object_suffix (str): Eg; json or xlsx.

    Returns:
        list: List of pd.DataFrame
    """
    all_data = []
    for f in list_objects(path=s3_path, suffix=object_suffix):
        logger.info(f'Processing file {f}')
        if object_suffix == '.xlsx':
            raw_data = read_excel(path=f, skiprows=skiprows)
        elif object_suffix == '.json':
            raw_data = read_json(path=f, lines=True)
        logger.info('Adding column "source_id"')
        raw_data['source_id'] = os.path.basename(f)
        logger.info('Adding column "reporting_date"')
        raw_data['reporting_date'] = extract_date_from_filename(f, sep)
        all_data.append(raw_data)
        clean_from_memory(raw_data)
    return all_data


def process_raw_files(dag_config: Variable,
                      object_suffix: str,
                      ti: str,
                      skiprows: int = None,
                      filtering: bool = False):
    """Reads the previously downloaded files in S3 into a Pandas DataFrame,
    and adds "source_id" column as a unique identifier. This is the filename
    as from the remote sftp server. The transformed data is written to
    s3 in parquet format.
    """
    config = Variable.get(dag_config, deserialize_json=True)
    s3_processed_destination_path = config.get('s3_processed_destination_path')
    glue_table = config.get('glue_table')
    glue_database = config.get('glue_database')
    s3_input_partition = ti.xcom_pull(key='s3_written_partition')
    logger.info(f'Reading all files from {s3_input_partition} into a DataFrame')
    processed_data = transform_raw_source_files(s3_path=s3_input_partition,
                                                object_suffix=object_suffix,
                                                skiprows=skiprows)
    concat_data = pd.concat(processed_data)
    clean_from_memory(processed_data)

    if filtering:
        logger.info('Filtering out invalid values from the DataFrame')
        filtered_data = concat_data[concat_data["Item No"].str.startswith('GRB')].copy()
        clean_from_memory(concat_data)
    else:
        filtered_data = concat_data

    logger.info('Adding batch_id, extracted_at, and partition columns to the DataFrame')
    final_data, batch_id = transform_dataframe(input_df=filtered_data)
    logger.info('Writing dataframe to S3 as parquet')
    s3_writer = rewrite_data(s3_destination_path=s3_processed_destination_path,
                             glue_table_name=glue_table,
                             glue_database=glue_database,
                             final_data=final_data,
                             partition_cols=['year', 'month', 'day', 'hour'])

    logger.info('Pushing batch_id, partition values and s3 output path to XCOM')
    ti.xcom_push(key="batch_id", value=batch_id)
    for partition in s3_writer['partitions_values'].values():
        ti.xcom_push(key="year", value=partition[0])
        ti.xcom_push(key="month", value=partition[1])
        ti.xcom_push(key="day", value=partition[2])
        ti.xcom_push(key="hour", value=partition[3])

    return s3_writer['paths'][0]


def process_raw_grading_files(dag_config: Variable,
                              object_suffix: str,
                              ti: str,
                              sep: str = '-',
                              columns: list = None):
    """Reads the previously downloaded files in S3 into a Pandas DataFrame,
    and adds "source_id" column as a unique identifier. This is the filename
    as from the remote sftp server. The transformed data is written to
    s3 in parquet format.
    """
    config = Variable.get(dag_config, deserialize_json=True)
    s3_processed_destination_path = config.get('s3_processed_destination_path')
    glue_table = config.get('glue_table')
    glue_database = config.get('glue_database')
    s3_input_partition = ti.xcom_pull(key='s3_written_partition')
    logger.info(f'Reading all files from {s3_input_partition} into a DataFrame')
    processed_data = transform_raw_source_files(s3_path=s3_input_partition,
                                                object_suffix=object_suffix,
                                                sep=sep)
    concat_data = pd.concat(processed_data)
    clean_from_memory(processed_data)

    logger.info('Renaming timestamp column to source_timestamp')
    concat_data.rename(columns={'timestamp': 'source_timestamp'}, inplace=True)

    logger.info('Encoding JSON fields')
    concat_data_json = global_df_json_encoder(concat_data)
    clean_from_memory(concat_data)

    logger.info('Appending missing columns with NULL values')
    concat_data_full = concat_data_json.reindex(columns=columns)
    clean_from_memory(concat_data_json)

    logger.info('Adding batch_id, extracted_at, and partition columns to the DataFrame')
    final_data, batch_id = transform_dataframe(input_df=concat_data_full)
    logger.info('Writing dataframe to S3 as parquet')
    s3_writer = rewrite_data(s3_destination_path=s3_processed_destination_path,
                             glue_table_name=glue_table,
                             glue_database=glue_database,
                             final_data=final_data,
                             partition_cols=['year', 'month', 'day', 'hour'])

    logger.info('Pushing batch_id, partition values and s3 output path to XCOM')
    ti.xcom_push(key="batch_id", value=batch_id)
    for partition in s3_writer['partitions_values'].values():
        ti.xcom_push(key="year", value=partition[0])
        ti.xcom_push(key="month", value=partition[1])
        ti.xcom_push(key="day", value=partition[2])
        ti.xcom_push(key="hour", value=partition[3])

    return s3_writer['paths'][0]
