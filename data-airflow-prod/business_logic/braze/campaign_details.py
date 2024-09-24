import asyncio
import json
import logging
from datetime import date, timedelta

import pandas as pd
from airflow.models import Variable
from awswrangler.s3 import read_csv, read_parquet, to_csv

from business_logic.braze.config.column_list import campaign_columns
from plugins.s3_utils import get_s3_full_path
from plugins.utils.braze.export_utils import (fetch_many, get_list,
                                              transform_dataframe)
from plugins.utils.data_lake_helper import rewrite_data

logger = logging.getLogger(__name__)

BRAZE_API_KEY_VAR_NAME = 'braze_api_key'
BRAZE_BASE_URL = 'braze_campaign_url'


def fetch_all_campaign_ids(url: str,
                           headers: dict,
                           last_edited_date: str,
                           page: int = 0,
                           sort_direction: str = 'desc',
                           include_archived: str = 'false') -> list:

    """Loop GET requests through the campaign list URL to retrieve all
    recently updated campaigns. When the end page is reached, the loop
    exits.

    Args:
        url (str): Campaign list URL.
        headers (dict): API authentication token.
        last_edited_date (str): Timestamp used for sorting the campaings.
        page (int, optional): Paginating through the GET requests.
        sort_direction (str, optional): Sorting the request results.
        include_archived (str, optional): To skip or GET campaigns which were
        disabled in Braze.

    Returns:
        list: Appended list of campaigns
    """

    all_campaign_ids, new_results = [], True
    while new_results:
        params = {
                    'page': page,
                    'sort_direction': sort_direction,
                    'include_archived': include_archived,
                    'last_edit.time[gt]': last_edited_date
                }
        campaigns = get_list(url=url, headers=headers, params=params, braze_export_type='campaigns')
        new_results = [k['id'] for k in campaigns]
        all_campaign_ids.extend(new_results)
        page += 1

    return all_campaign_ids


def build_message_body(campaign_base: dict, messages: dict) -> dict:
    """The "messages" field in the campaign details are concatenated as a json
    array, this function denests it and retrieves the required fields. Then merges
    the new arrays with the orignal campaign_id. This creates a 1-to-many mapping
    between campaign_id and message_variation_id.

    Args:
        campaign_base (dict): Dict containing campaign details
        messages (dict): Nested dict of campaign['messages']

    Returns:
        dict: Merged dict of campaigns and its related messages.
    """
    message_body = []
    for k, v in messages.items():
        message = {
            'message_variation_id': 'NULL' if not k else k,
            'message_variation_channel': 'NULL' if not v.get('channel') else v['channel'],
            'message_variation_name': 'NULL' if not v.get('name') else v['name']
            }
        merged_dict = {**campaign_base, **message}
        message_body.append(merged_dict)
    return message_body


def build_campaign_messages(records: list) -> list:
    """From the list of campaign details, each record is broken down to extract the
    required fields. The campaign['message'] field is a nested dict, and this is
    denested in the called function.

    Args:
        records (list): List of dicts containing campaign details

    Returns:
        list: Appended list of merged dicts containing campaign details and messages
    """
    full_records = []
    for record in records:
        campaign_base = {
            'campaign_id': record['campaign_id'],
            'campaign_name': record['name'],
            'created_at': record['created_at'],
            'updated_at': record['updated_at'],
            'description': record['description'],
            'channels': record['channels'],
            'tags': record['tags']
            }
        full_records.extend(build_message_body(campaign_base=campaign_base,
                                               messages=json.loads(record.get('messages', []))))
    return full_records


def extract_campaign_ids_to_s3(**context):
    config = Variable.get('braze_campaign_details_settings', deserialize_json=True)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix_campaign_list']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)

    default_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    last_edited_date = config.get('last_edited_date', default_date)

    url = f'{Variable.get(BRAZE_BASE_URL)}/list'
    headers = {
                "Authorization": "Bearer " + Variable.get(BRAZE_API_KEY_VAR_NAME)
              }

    logger.info('Getting list of campaigns from Braze')
    campaign_ids = fetch_all_campaign_ids(url=url,
                                          headers=headers,
                                          last_edited_date=last_edited_date)

    if len(campaign_ids) == 0:
        logger.info(f'No campaigns found with edited date >= {last_edited_date}, exiting DAG...')
        return False
    else:
        logger.info(f'Campaigns found with edited date >= {last_edited_date}: {len(campaign_ids)}')

    logger.info('Writing list of campaign IDs to S3')
    s3_writer = to_csv(df=pd.DataFrame(campaign_ids),
                       path=s3_destination_path,
                       sep=',',
                       index=False,
                       dataset=True,
                       mode='overwrite')

    logger.info('Pushing S3 written path to XCOM')
    task_instance = context['ti']
    task_instance.xcom_push(key='s3_path_campaign_id', value=s3_writer['paths'][0])

    return True


def extract_raw_campaign_details_to_s3(**context):
    config = Variable.get('braze_campaign_details_settings', deserialize_json=True)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix_campaign_details_raw']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    glue_table = config['glue_table_raw']

    task_instance = context['ti']
    campaign_id_data = read_csv(path=task_instance.xcom_pull(key='s3_path_campaign_id',
                                                             task_ids='extract_campaign_ids_to_s3'))
    headers = {
                "Authorization": "Bearer " + Variable.get(BRAZE_API_KEY_VAR_NAME)
              }

    campaign_ids = campaign_id_data['0'].values.tolist()
    results = asyncio.run(
                    fetch_many(
                        iterator=campaign_ids,
                        url='https://rest.fra-01.braze.eu/campaigns/details?campaign_id={}',
                        headers=headers,
                        braze_export_type_id="campaign_id"))

    logger.info('Moving `campaign_id` column to the first position')
    results_df = pd.DataFrame(results)
    campaign_data = results_df[['campaign_id'] +
                               [col for col in results_df.columns if col != 'campaign_id']]

    logger.info('Adding batch_id, extracted_at, and partition columns to the DataFrame')
    campaign_data, batch_id_value = transform_dataframe(input_df=campaign_data)

    task_instance.xcom_push(key='batch_id_value', value=batch_id_value)

    logger.info('Writing raw campaign details to S3')
    s3_writer = rewrite_data(s3_destination_path=s3_destination_path,
                             glue_table_name=glue_table,
                             glue_database=glue_database,
                             final_data=campaign_data,
                             partition_cols=['year', 'month', 'day', 'batch_id'])

    task_instance.xcom_push(key='s3_path_campaign_data_raw', value=s3_writer['paths'][0])


def write_curated_campaign_details_to_s3(**context):
    config = Variable.get('braze_campaign_details_settings', deserialize_json=True)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix_campaign_details_curated']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    glue_table = config['glue_table_curated']

    task_instance = context['ti']

    logger.info('Reading raw campaign details data from S3')
    s3_path = task_instance.xcom_pull(key='s3_path_campaign_data_raw',
                                      task_ids='extract_raw_campaign_details_to_s3')
    raw_campaign_data = read_parquet(path=s3_path, columns=campaign_columns)

    logger.info('Converting campaign DataFrame to list of dicts')
    records = raw_campaign_data.to_dict('records')

    logger.info('Building the curated campaign details')
    full_records = build_campaign_messages(records=records)

    logger.info('Adding the current batch_id and partition columns')
    batch_id_value = task_instance.xcom_pull(key='batch_id_value',
                                             task_ids='extract_raw_campaign_details_to_s3')
    curated_data, _ = transform_dataframe(input_df=pd.DataFrame(full_records),
                                          batch_id_value=batch_id_value)

    logger.info('Writing curated campaign details to S3')
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
