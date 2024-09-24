import json
import logging
import ntpath
import os
from urllib.parse import urlparse

import boto3
import numpy as np
import pandas as pd
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.sftp.hooks.sftp import SFTPHook

from business_logic.recommerce.ingram_micro.commons import \
    process_raw_grading_files
from plugins.utils.recommerce import write_delta_filelist_s3

logger = logging.getLogger(__name__)

target_columns = ['order_number',
                  'serial_number',
                  'item_number',
                  'shipping_number',
                  'disposition_code',
                  'status_code',
                  'source_timestamp',
                  'partner_id',
                  'asset_serial_number',
                  'package_serial_number',
                  'questions',
                  'shipment_type',
                  'date_of_processing',
                  'carrier_out',
                  'carrier_service',
                  'track_and_trace',
                  'customer',
                  'country',
                  'package_no',
                  'serial_no',
                  'assetname',
                  'damage',
                  'hc_code',
                  'item_category',
                  'reporting_date',
                  'source_id',
                  'added_items',
                  'awaited_parts',
                  'new_serial_number'
                  ]


def process_and_transform_raw_files(dag_config, **context):
    ti = context['ti']
    s3_processed_partition = process_raw_grading_files(dag_config=dag_config,
                                                       object_suffix='.json',
                                                       ti=ti,
                                                       columns=target_columns)
    return s3_processed_partition


def list_remote_server_files(sftp_conn_id: str, remote_path: str) -> list:
    """Get list of files in the sftp remote server path"""
    client = SFTPHook(ssh_conn_id=sftp_conn_id)
    folders = client.list_directory(path=remote_path)
    files_in_remote_path = []
    if 'test' in folders:
        folders.remove('test')
    # to read the files in the 5 most recently created folders
    for i in range(1, 6):
        logger.info("Getting list of files in sftp server:" + remote_path + "/" + folders[-1*i])
        files = client.list_directory(remote_path + "/" + folders[-1*i])
        for file in files:
            files_in_remote_path.append(folders[-1*i]+"/" + file)
    return files_in_remote_path


def get_alchemy_connection(redshift_conn_id: str):
    logger.info('Establishing connection to Redshift')
    src_postgres_con = RedshiftSQLHook(aws_conn_id=redshift_conn_id).get_sqlalchemy_engine()
    logger.info('Connection is successful')
    return src_postgres_con


def get_source_files_from_redshift(conn_id):
    engine = get_alchemy_connection(conn_id)
    query = """
    SELECT source_id
    FROM recommerce.ingram_micro_send_order_grading_status
    WHERE extracted_at::date>=current_date - 10
    """
    logger.info('Executing:')
    logger.info(query)

    df = pd.read_sql_query(
        sql=query,
        con=engine)
    files_in_redshift = [file for file in df['source_id']]
    return files_in_redshift


def get_delta_source_files_names(remote_server_path: str, redshift_conn_id: str,
                                 source_sftp_conn_id: str):
    files_in_sftp = list_remote_server_files(sftp_conn_id=source_sftp_conn_id,
                                             remote_path=remote_server_path)
    files_in_redshift = get_source_files_from_redshift(redshift_conn_id)
    diff = []
    """as files_in_sftp has folder name as prefix,
    prefix needs to be removed while comparing the files"""
    for file in files_in_sftp:
        if ntpath.basename(file) not in files_in_redshift:
            diff.append(file)
    length_of_list = len(diff)
    logger.info("Number of files to be downloaded:", length_of_list)
    return diff


def get_bucket_and_prefix(path: str):
    parsed_url = urlparse(path)
    bucket = parsed_url.netloc
    pre_prefix = os.path.dirname(path)
    prefix = urlparse(pre_prefix).path
    key = parsed_url.path
    return [bucket, prefix, key]


def get_source_files_delta(dag_config: Variable,
                           redshift_conn_id: str,
                           source_sftp_conn_id: str,
                           **context):
    """Compares the files in the remote sftp server against the available files
    in Redshift recommerce.ingram_micro_send_order_grading_status table.
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
    remote_server_path = config.get('remote_server_path')
    source_files_to_download_csv_path = config.get('source_files_to_download_csv_path')

    delta_filenames = get_delta_source_files_names(remote_server_path,
                                                   redshift_conn_id, source_sftp_conn_id)
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


def transform_raw_ingram_source_files(**context):
    engine = RedshiftSQLHook('redshift_default').get_sqlalchemy_engine()
    client = boto3.client('s3')
    ti = context['ti']
    s3_input_partition = ti.xcom_pull(key='s3_written_partition')
    logger.info(f'Reading all files from {s3_input_partition} into a DataFrame')
    s3_bucket = get_bucket_and_prefix(s3_input_partition)[0]
    prefix = get_bucket_and_prefix(s3_input_partition)[2][1:]+"/"
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=prefix)

    all_data = pd.DataFrame()
    for page in pages:
        for obj in page['Contents']:
            logger.info("Reading file:" + obj['Key'])
            file = obj['Key']
            extension = os.path.splitext(file)
            if extension[1] == '.json':
                file_name = ntpath.basename(file)
                file_obj = client.get_object(Bucket=s3_bucket, Key=file)
                obj_head = client.head_object(Bucket=s3_bucket, Key=file)
                file_size = obj_head['ContentLength']
                if file_size > 0:
                    file_content = file_obj['Body'].read().decode('utf-8')
                    json_content = json.loads(file_content)
                    if len(json_content) < 3:
                        raw_data = pd.DataFrame(json_content.get('data').get('assets'))
                        raw_data['disposition_code'] = json_content.get('event_type')
                    else:
                        raw_data = pd.DataFrame([json_content])
                    raw_data['source_id'] = file_name
                else:
                    continue
                all_data = pd.concat([all_data, raw_data])
    logger.info(all_data.shape)
    all_data['reporting_date'] = pd.to_datetime(all_data['timestamp']).apply(lambda x: x.date())
    all_data.rename(columns={"timestamp": "source_timestamp"}, inplace=True)
    if 'customer_order_number' in all_data:
        all_data.drop(['customer_order_number'], axis=1, inplace=True)
    if 'license_plate' in all_data:
        all_data.drop(['license_plate'], axis=1, inplace=True)
    if 'questions' in all_data:
        all_data['questions'] = list(map(lambda x: json.dumps(x), all_data['questions']))
    if 'added_items' in all_data:
        all_data['added_items'] = list(map(lambda x: json.dumps(x), all_data['added_items']))
    if 'awaited_parts' in all_data:
        all_data['awaited_parts'] = list(map(lambda x: json.dumps(x), all_data['awaited_parts']))
    all_data = all_data.replace('NaN', '')
    all_data = all_data.replace(r'^\s*$', np.nan, regex=True)
    all_data = all_data.replace({pd.NaT: None, np.NaN: None, np.nan: None})
    all_data.to_sql(name='ingram_sftp_daily_events', con=engine, if_exists='append', index=False,
                    schema='staging', method="multi", chunksize=1000)
    return
