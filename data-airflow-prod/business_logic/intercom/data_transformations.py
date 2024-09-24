import logging
from datetime import datetime

import pandas as pd
import requests

from plugins.date_utils import convert_unix_ts
from plugins.utils.pandas_helpers import (add_inserted_at_columns,
                                          add_partition_columns,
                                          column_json_encoder,
                                          empty_cols_to_string_type,
                                          insert_batch_id,
                                          remove_trailing_zeros)

logger = logging.getLogger(__name__)


def get_page_count(headers: str, params: tuple, per_page: int) -> int:
    """Perform initial request to get the current total number of pages. Also returns
    the current number of conversations in Intercom for logging.
    """
    logger.info('Getting total number of pages in Intercom')
    try:
        response = requests.get(
            url='https://api.intercom.io/conversations?&page=1',
            headers=headers,
            params=params)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        logger.info(f'Response return error: {err}')
        raise SystemExit(err)

    response_json = response.json()
    total_pages = (response_json['pages']['total_pages'])
    logger.info('Total pages of Intercom conversations per '
                f'{per_page} conversations: {total_pages}')
    total_count = (response_json['total_count'])
    logger.info(f'Total number of conversations in Intercom currently: {total_count}')

    return total_pages


def get_init_starting_after(headers: str, params: tuple) -> int:
    """Perform initial request to get the starting_after of first page.
    """
    logger.info('Getting starting_after of first page of Intercom')

    try:
        response = requests.get(
            url='https://api.intercom.io/conversations?&page=1',
            headers=headers,
            params=params)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        logger.info(f'Response return error: {err}')
        raise SystemExit(err)

    response_json = response.json()
    starting_after = (response_json['pages']['next']['starting_after'])
    return starting_after


def get_starting_after(headers: str, params: tuple) -> int:
    """Perform after initial request to get the starting_after of other pages.
    """
    logger.info('Getting starting_after of after first page of Intercom')

    try:
        response = requests.get(
            url='https://api.intercom.io/conversations?&starting_after={}',
            headers=headers,
            params=params)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        logger.info(f'Response return error: {err}')
        raise SystemExit(err)

    response_json = response.json()
    starting_after = (response_json['pages']['next']['starting_after'])
    return starting_after


def transform_response(item: list) -> dict:
    """Extracts elements from a response list, and applies the date function
    to convert the unix timestamp to YYYY-MM-DD HH:MM:SS format.
    Additionally creates partition key:values
    """
    return {
        'id':  item['id'],
        'created_at': convert_unix_ts(item['created_at']),
        'updated_at': convert_unix_ts(item['updated_at']),
        'extracted_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'year': datetime.now().strftime("%Y"),
        'month': datetime.now().strftime("%m"),
        'day': datetime.now().strftime("%d")
        }


def split_conversations_parts(df: pd.DataFrame) -> pd.DataFrame:
    """Explode the "conversation_parts" json object to sub-objects
    due to column length. Drop the orignal column then concat the newly
    formed columns"""
    logger.info("Splitting 'conversation_parts' JSON column")
    add_df = pd.json_normalize(df['conversation_parts'])
    df.drop('conversation_parts', axis=1, inplace=True)
    add_df = add_df[[c for c in add_df.columns if c not in df.columns]]
    df = pd.concat([df, add_df], axis=1)
    return df


def split_source(df: pd.DataFrame) -> pd.DataFrame:
    """Explode the "source" json object to sub-objects due to column length.
    Drop the orignal column then concat the newly formed columns"""
    logger.info("Splitting 'source' JSON column")
    add_df = pd.json_normalize(df['source'], max_level=0)
    keep_list = ['attachments',
                 'author',
                 'body',
                 'delivered_as',
                 'id',
                 'subject',
                 'url',
                 'redacted']
    add_df = add_df[[c for c in add_df.columns if c in keep_list]]
    add_df.rename({'id': 'source_id'}, axis=1, inplace=True)
    df.drop('source', axis=1, inplace=True)
    df = pd.concat([df, add_df], axis=1)
    return df


def transform_dataframe(df: pd.DataFrame, columns: list, batch_id_value: str) -> pd.DataFrame:
    """Adds extracted_at and batch_id columns, and partitions using the pandas_helpers
    plugin utility.
    """
    logger.info("Transforming dataframe")
    df = split_source(df)
    df = split_conversations_parts(df)
    df = column_json_encoder(df, columns, ensure_ascii=False)
    df = empty_cols_to_string_type(df, all_columns_as_string=True)
    logger.info("Removing trailing zeros")
    for col in ['waiting_since',
                'snoozed_until',
                'admin_assignee_id',
                'team_assignee_id']:
        df = remove_trailing_zeros(df, col)
    df, _ = insert_batch_id(df, 'batch_id', batch_id_value)
    df = add_inserted_at_columns(df, 'extracted_at')
    df = add_partition_columns(df, 'extracted_at', ['year', 'month', 'day'])
    return df
