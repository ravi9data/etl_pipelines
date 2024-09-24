import logging

import pandas as pd
import requests
from airflow.models import Variable
from requests.auth import HTTPBasicAuth

from business_logic.ixopay.columns import keep_columns
from plugins.s3_utils import get_s3_full_path
from plugins.utils.data_lake_helper import rewrite_data
from plugins.utils.memory import clean_from_memory
from plugins.utils.pandas_helpers import (empty_cols_to_string_type,
                                          global_df_json_encoder,
                                          prepare_df_for_s3)

logger = logging.getLogger(__name__)


IXOPAY_USERNAME = 'ixopay_username'
IXOPAY_API_KEY = 'ixopay_api_key'
URL = 'https://bds.ixopay.com/query/transactions'


def ixopay_request(interval_hour: str, request_size: int) -> list:
    """Send one POST request to Ixopay API using HTTPBasicAuth. The query body gets the data for the
    last x hours compared to the current time. The response output is filtered to get the relevant
    data.

    Returns a list of JSON objects.
    """
    body = {"query":
            {"bool":
             {"filter":
              {"range":
               {"created_at":
                {"gte": "now-"+interval_hour, "lte": "now"}}}}},
            "size": request_size}

    resp = requests.get(
                url=URL,
                auth=HTTPBasicAuth(Variable.get(IXOPAY_USERNAME), Variable.get(IXOPAY_API_KEY)),
                json=body)
    resp.raise_for_status()

    return resp.json()['hits']['hits']


def transform_dataframe(input_df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """Only a subset of the columns are required, so columns not present in the column list are dropped.
    The remaining dataframe is properly JSON encoded, and the batch_id and partition columns are
    added.

    :returns:
    The transformed dataframe
    Generated batch_id value
    """
    logger.info('Filtering Dataframe columns based on expected list of columns')
    cleaned_df = input_df.reindex(keep_columns, axis=1, fill_value='')
    clean_from_memory(input_df)

    try:
        assert len(cleaned_df.columns) == len(keep_columns)
    except AssertionError as err:
        logger.error("Dataframe columns do not match expected list of columns")
        raise RuntimeError(err)

    cleaned_df = global_df_json_encoder(cleaned_df)
    cleaned_df = empty_cols_to_string_type(cleaned_df, True)
    cleaned_df, batch_id = prepare_df_for_s3(cleaned_df)

    return cleaned_df, batch_id


def extract_data_to_s3(**context):
    """
    Performs the POST request to Ixopay API, and filters the output to a Pandas Dataframe. This is
    written to S3, and the batch_id and partition values are passed to XCOM for the subsequent sql
    scripts. We keep the extraction history in the target Glue table, and the latest records and be
    identified by the `extracted_at` column.
    """
    config = Variable.get('ixopay_transactions_extract_settings', deserialize_json=True)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    glue_table = config['glue_table']
    interval_hour = config['interval_hour']
    request_size = int(config['request_size'])

    logger.info('Extracting data from Ixopay API')
    resp_data = ixopay_request(interval_hour, request_size)

    logger.info('Converting data to a dataframe')
    transaction_data = pd.DataFrame([d['_source'] for d in resp_data])
    logger.info(f'Number of records retrieved: {transaction_data.shape[0]}')

    logger.info('Transforming dataframe for writing to S3')
    transaction_data, batch_id = transform_dataframe(transaction_data)

    logger.info('Writing dataframe to S3')
    s3_writer = rewrite_data(
                    s3_destination_path=s3_destination_path,
                    glue_table_name=glue_table,
                    glue_database=glue_database,
                    final_data=transaction_data)

    logger.info('Pushing batch_id and partition values to XCOM')
    task_instance = context['ti']
    task_instance.xcom_push(key="batch_id", value=batch_id)
    for partition in s3_writer['partitions_values'].values():
        task_instance.xcom_push(key="year", value=partition[0])
        task_instance.xcom_push(key="month", value=partition[1])
        task_instance.xcom_push(key="day", value=partition[2])

    return s3_writer['paths'][0]
