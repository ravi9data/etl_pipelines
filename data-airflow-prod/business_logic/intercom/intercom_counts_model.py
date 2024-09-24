import logging

import pandas as pd
import requests
from airflow.models import Variable

from plugins import s3_utils
from plugins.utils.data_lake_helper import s3_writer_without_partitions

logger = logging.getLogger(__name__)

INTERCOM_API_KEY = 'intercom_api_key_v2.8'


def intercom_api_get(url, header_token, header_accept, response_filter1, response_filter2=None):
    """
    Sends a GET request to Intercom API endpoint to retrieve the specified data. Depending on the
    desired target, the return response would be filtered. If the request fails (HTTP error),
    the function throws an error.

    Args:
        c_url: URL endpoint
        header_token: Authorization key for Grover authentication
        header_accept: Type of response expected
        response_filter1: Filter the response output
        response_filter2: Optional, filter the response output further

    Returns:
        Pandas dataframe containing the GET response data
    """
    headers = dict(Authorization=header_token, Accept=header_accept)
    response = requests.get(url=url, headers=headers)
    response.raise_for_status()

    if response_filter2:
        response_data = response.json()[response_filter1][response_filter2]
    else:
        response_data = response.json()[response_filter1]

    if isinstance(response_data, dict):
        response_df = pd.DataFrame(response_data, index=[0])
    else:
        response_df = pd.DataFrame(response_data, dtype=str)
    return response_df


def counts_model_writer(url,
                        glue_table_name,
                        s3_bucket_prefix,
                        response_filter1,
                        response_filter2=None,
                        **kwargs):
    """
    Gets runtime parameters from the UI Variable, and config list. These are passed to
    the sub-functions to perform the API request, and write the response to S3.

    """
    config = Variable.get('intercom_counts_model_settings', deserialize_json=True)
    s3_destination_bucket = config['s3_bucket']
    s3_destination_path = s3_utils.get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    header_token = Variable.get(INTERCOM_API_KEY)
    header_accept = config['header_accept']

    if response_filter2:
        logger.info(f'Extracting data from {url} with response filter "{response_filter1}" \
        and "{response_filter2}"')
        df = intercom_api_get(
            url=url,
            header_token=header_token,
            header_accept=header_accept,
            response_filter1=response_filter1,
            response_filter2=response_filter2
        )
    else:
        logger.info(f'Extracting data from {url} with response filter "{response_filter1}"')
        df = intercom_api_get(
            url=url,
            header_token=header_token,
            header_accept=header_accept,
            response_filter1=response_filter1
        )

    s3_written_data = s3_writer_without_partitions(
        df=df,
        s3_destination_path=s3_destination_path,
        glue_table_name=glue_table_name,
        glue_database=glue_database,
        mode='overwrite'
        )

    return s3_written_data
