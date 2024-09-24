import logging

import pandas as pd
from airflow.models import Variable
from awswrangler.s3 import copy_objects, read_csv, to_csv

from plugins.s3_utils import list_objects_without_prefix
from plugins.utils.slack import send_notification

logger = logging.getLogger(__name__)


def write_delta_filelist_s3(delta_files: set, s3_src_path: str, s3_csv_path: str) -> str:
    """_summary_

    Args:
        delta_files (set): Set of filenames, the diff between s3 source & dest path.
        s3_dest_path (str): Target directory
        s3_base_path (str): Path to write the csv of delta filenames in s3

        Appends the s3_base_path to the filename, e.g.;
        "s3://abc/"+"123.json" -> "s3://path/abc/123.json",
        and writes this as a csv file in s3.

    Returns:
        str: Written file location in s3 stored as csv
    """
    data = pd.DataFrame(data=[s3_src_path + f for f in list(delta_files)],
                        columns=['source_filename'])
    s3_writer = to_csv(df=data, path=s3_csv_path, index=False)
    return s3_writer['paths'][0]


def get_files_diff(source_files: list, dest_files: list) -> set:
    """Given 2 list inputs, transform to set and get the diff. Compares only the filenames,
    must be without any prefix or directory path.
    Args:
        source_files (list): List of objects in the source path.
        dest_files (list):  List of objects in the destination path.
    Returns:
        set: A set of filename differences
    """
    logger.info('Getting filenames differences between sftp server and S3.')
    return set(source_files) - set(dest_files)


def compare_files(s3_src_path: str, s3_dest_path: str) -> set:
    s3_src_files = list_objects_without_prefix(s3_path=s3_src_path)
    s3_dest_files = list_objects_without_prefix(s3_path=s3_dest_path)
    delta_files = get_files_diff(source_files=s3_src_files, dest_files=s3_dest_files)
    return delta_files


def move_files_to_dest(delta_files: list, s3_src_path: str, s3_dest_path: str):
    written_files = copy_objects(paths=get_filenames_list(s3_path=delta_files),
                                 source_path=s3_src_path,
                                 target_path=s3_dest_path)
    return written_files


def get_filenames_list(s3_path: str) -> list:
    logger.info(f'Reading filenames to copy from {s3_path}')
    delta_filenames = read_csv(path=s3_path)
    logger.info(f'Number of files to be copied: {delta_filenames.shape[0]}')
    return list(delta_filenames.iloc[:, 0])


def get_latest_source_files(dag_config: Variable, **context):
    """Compares the files in the source bucket against the current files in the
    destination bucket. If there are no new files, then exit. Else, writes the list
    of delta filenames to one csv file in s3, and move the objects to the destination
    bucket.

    The output csv files consists of a list of delta filenames to be downloaded. This file
    is overwritten each run.
    """
    config = Variable.get(dag_config, deserialize_json=True)
    s3_src_path = config.get('s3_src_path')
    s3_dest_path = config.get('s3_dest_path')
    s3_csv_path = config.get('s3_csv_path')

    logger.info(f'Comparing existing files in {s3_src_path} against'
                f' files available in "{s3_dest_path}".')
    delta_filenames = compare_files(s3_src_path=s3_src_path, s3_dest_path=s3_dest_path)
    if not delta_filenames:
        logger.info('No delta files present this run, skipping tasks.')
        return 'slack_message_no_delta'
    else:
        logger.info(f'We have {len(delta_filenames)} file(s) to load for this run.')
        s3_written_path = write_delta_filelist_s3(delta_files=delta_filenames,
                                                  s3_src_path=s3_src_path,
                                                  s3_csv_path=s3_csv_path)
        logger.info(f'Delta filenames written to "{s3_written_path}"')
        return 'copy_source_files'


def slack_message_no_delta(dag_config: Variable):
    """Uses the slack_utils to send a Slack message to the
    #dataload_notifications channel if there are no new or
    files available from the sftp server.
    """
    config = Variable.get(dag_config, deserialize_json=True)
    msg_topic = config.get('slack_message_no_delta_topic')
    message = (f"*{msg_topic}:* No new files available for this run.")
    send_notification(message=message, slack_connection_id='slack_dataload_notifications')
