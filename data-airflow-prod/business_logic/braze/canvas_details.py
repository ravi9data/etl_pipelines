import asyncio
import json
import logging
from datetime import date, timedelta

import pandas as pd
from airflow.models import Variable
from awswrangler.s3 import read_csv, read_parquet, to_csv

from business_logic.braze.config.column_list import canvas_columns
from plugins.s3_utils import get_s3_full_path
from plugins.utils.braze.export_utils import (fetch_many, get_list,
                                              transform_dataframe)
from plugins.utils.data_lake_helper import rewrite_data

logger = logging.getLogger(__name__)

BRAZE_API_KEY_VAR_NAME = 'braze_api_key'
BRAZE_BASE_URL = 'braze_canvas_url'


def fetch_all_canvas_ids(url: str,
                         headers: dict,
                         last_edited_date: str,
                         page: int = 0,
                         sort_direction: str = 'desc',
                         include_archived: str = 'false') -> list:

    """Loop GET requests through the canvas list URL to retrieve all
    recently updated canvas. When the end page is reached, the loop
    exits.
    Args:
        url (str): canvas list URL.
        headers (dict): API authentication token.
        last_edited_date (str): Timestamp used for sorting the canvas.
        page (int, optional): Paginating through the GET requests.
        sort_direction (str, optional): Sorting the request results.
        include_archived (str, optional): To skip or GET canvas which were
        disabled in Braze.
    Returns:
        list: Appended list of canvas
    """

    all_canvas_ids, new_results = [], True
    while new_results:
        params = {
                    'page': page,
                    'sort_direction': sort_direction,
                    'include_archived': include_archived,
                    'last_edit.time[gt]': last_edited_date
                }
        canvas = get_list(url=url, headers=headers, params=params, braze_export_type="canvases")
        new_results = [k['id'] for k in canvas]
        all_canvas_ids.extend(new_results)
        page += 1

    return all_canvas_ids


def build_message_body(canvas_base: dict, steps: dict) -> dict:
    """The "messages" field in the canvas details are concatenated as a json
    array, this function denests it and retrieves the required fields. Then merges
    the new arrays with the orignal canvas_id. This creates a 1-to-many mapping
    between canvas_id and message_variation_id.
    Args:
        canvas_base (dict): Dict containing canvas details
        messages (dict): Nested dict of canvas['messages']
    Returns:
        dict: Merged dict of canvas and its related messages.
    """
    message_body = []
    for v in steps:
        message = {
            'steps_name': 'NULL' if not v.get('name') else v['name'],
            'steps_id': 'NULL' if not v.get('id') else v['id'],
            'next_step_ids': 'NULL' if not v.get('next_step_ids') else v['next_step_ids'],
            'steps_channels': 'NULL' if not v.get('channels') else v['channels']
            }
        merged_dict = {**canvas_base, **message}
        message_body.append(merged_dict)
    return message_body


def build_canvas_messages(records: list) -> list:
    """From the list of canvas details, each record is broken down to extract the
    required fields. The canvas['message'] field is a nested dict, and this is
    denested in the called function.
    Args:
        records (list): List of dicts containing canvas details
    Returns:
        list: Appended list of merged dicts containing canvas details and messages
    """
    full_records = []
    for record in records:
        canvas_base = {
            'canvas_id': record['canvas_id'],
            'created_at': record['created_at'],
            'updated_at': record['updated_at'],
            'canvas_name': record['name'],
            'description': record['description'],
            'archived': record['archived'],
            'draft': record['draft'],
            'schedule_type': record['schedule_type'],
            'first_entry': record['first_entry'],
            'last_entry': record['last_entry'],
            'channels': record['channels'],
            'tags': record['tags'],
            'variants': record['variants'],
            'steps': record['steps']
            }
        full_records.extend(build_message_body(canvas_base=canvas_base,
                                               steps=json.loads(record.get('steps', []))))
    return full_records


def extract_canvas_ids_to_s3(**context):
    config = Variable.get('braze_canvas_details_settings', deserialize_json=True)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix_canvas_list']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)

    default_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    last_edited_date = config.get('last_edited_date', default_date)

    url = f'{Variable.get(BRAZE_BASE_URL)}/list'
    headers = {
                "Authorization": "Bearer " + Variable.get(BRAZE_API_KEY_VAR_NAME)
              }

    logger.info('Getting list of canvas from Braze')
    canvas_ids = fetch_all_canvas_ids(url=url,
                                      headers=headers,
                                      last_edited_date=last_edited_date)

    if len(canvas_ids) == 0:
        logger.info(f'No canvas found with edited date >= {last_edited_date}, exiting DAG...')
        return False
    else:
        logger.info(f'canvas found with edited date >= {last_edited_date}: {len(canvas_ids)}')

    logger.info('Writing list of canvas IDs to S3')
    s3_writer = to_csv(df=pd.DataFrame(canvas_ids),
                       path=s3_destination_path,
                       sep=',',
                       index=False,
                       dataset=True,
                       mode='overwrite')

    logger.info('Pushing S3 written path to XCOM')
    task_instance = context['ti']
    task_instance.xcom_push(key='s3_path_canvas_id', value=s3_writer['paths'][0])

    return True


def extract_raw_canvas_details_to_s3(**context):
    config = Variable.get('braze_canvas_details_settings', deserialize_json=True)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix_canvas_details_raw']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    glue_table = config['glue_table_raw']

    task_instance = context['ti']
    canvas_id_data = read_csv(path=task_instance.xcom_pull(key='s3_path_canvas_id',
                                                           task_ids='extract_canvas_ids_to_s3'))
    headers = {
                "Authorization": "Bearer " + Variable.get(BRAZE_API_KEY_VAR_NAME)
              }

    canvas_ids = canvas_id_data['0'].values.tolist()
    results = asyncio.run(
                    fetch_many(
                        iterator=canvas_ids,
                        url='https://rest.fra-01.braze.eu/canvas/details?canvas_id={}',
                        headers=headers,
                        braze_export_type_id="canvas_id"))

    logger.info('Moving `canvas_id` column to the first position')
    results_df = pd.DataFrame(results)
    canvas_data = results_df[['canvas_id'] +
                             [col for col in results_df.columns if col != 'canvas_id']]

    logger.info('Adding batch_id, extracted_at, and partition columns to the DataFrame')
    canvas_data, batch_id_value = transform_dataframe(input_df=canvas_data)

    task_instance.xcom_push(key='batch_id_value', value=batch_id_value)

    logger.info('Writing raw canvas details to S3')
    s3_writer = rewrite_data(s3_destination_path=s3_destination_path,
                             glue_table_name=glue_table,
                             glue_database=glue_database,
                             final_data=canvas_data,
                             partition_cols=['year', 'month', 'day', 'batch_id'])

    task_instance.xcom_push(key='s3_path_canvas_data_raw', value=s3_writer['paths'][0])


def write_curated_canvas_details_to_s3(**context):
    config = Variable.get('braze_canvas_details_settings', deserialize_json=True)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix_canvas_details_curated']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    glue_table = config['glue_table_curated']

    task_instance = context['ti']

    logger.info('Reading raw canvas details data from S3')
    s3_path = task_instance.xcom_pull(key='s3_path_canvas_data_raw',
                                      task_ids='extract_raw_canvas_details_to_s3')
    raw_canvas_data = read_parquet(path=s3_path, columns=canvas_columns)

    logger.info('Converting canvas DataFrame to list of dicts')
    records = raw_canvas_data.to_dict('records')

    logger.info('Building the curated canvas details')
    full_records = build_canvas_messages(records=records)

    logger.info('Adding the current batch_id and partition columns')
    batch_id_value = task_instance.xcom_pull(key='batch_id_value',
                                             task_ids='extract_raw_canvas_details_to_s3')
    curated_data, _ = transform_dataframe(input_df=pd.DataFrame(full_records),
                                          batch_id_value=batch_id_value)

    logger.info('Writing curated canvas details to S3')
    s3_writer = rewrite_data(s3_destination_path=s3_destination_path,
                             glue_table_name=glue_table,
                             glue_database=glue_database,
                             final_data=curated_data,
                             partition_cols=['year', 'month', 'day', 'batch_id'])

    logger.info('Pushing partition values to XCOM')
    for partition in s3_writer['partitions_values'].values():
        task_instance.xcom_push(key="year", value=partition[0])
        task_instance.xcom_push(key="month", value=partition[1])
        task_instance.xcom_push(key="day", value=partition[2])

    return s3_writer['paths'][0]
