import logging

import pandas as pd
import requests
from airflow.models import Variable

from plugins.voucherify_utils import get_iso_date_range, write_csv_file_s3

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_list_validation_rules(application_id,
                              client_secret_key,
                              params=None):
    """
    Makes a GET request to the Voucherify API to fetch validation rules.
    Args:
        application_id (str): The application ID for the API.
        client_secret_key (str): The client secret key for the API.
        params (dict, optional): Additional parameters for the API request.
    Returns:
        dict: The JSON response from the API containing validation rules.
    Raises:
        requests.exceptions.RequestException: If the request fails.
    """
    # Constants
    ENDPOINT_URL = 'https://api.voucherify.io/v1/validation-rules'
    TIMEOUT = 180
    headers = {
        'X-App-Id': application_id,
        'X-App-Token': client_secret_key,
        'X-Voucherify-Channel': 'Python-SDK',
        'Content-Type': 'application/json'
    }
    try:
        response = requests.get(ENDPOINT_URL,
                                headers=headers,
                                params=params,
                                timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exception:
        logger.error(f"Request failed: {exception}")
        raise


def fetch_all_validation_rules(application_id,
                               client_secret_key,
                               start_date):
    """
    Fetches all validation rules from the Voucherify API using pagination.
    Args:
        application_id (str): The application ID for the API.
        client_secret_key (str): The client secret key for the API.
        start_date (str): The start date for fetching validation rules.
    Returns:
        list: A list of all validation rules fetched from the API.
    Raises:
        Exception: If an error occurs during the API request.
    """
    all_validation_rules = []
    params = {
        "limit": 100,
        "page": 1,
        "start_date": start_date
    }

    while True:
        try:
            logger.info(f"Fetching page {params['page']}")
            response_data = get_list_validation_rules(application_id,
                                                      client_secret_key,
                                                      params=params)
            data = response_data.get("data", [])
            all_validation_rules.extend(data)

            if len(data) < params["limit"]:
                break
            params["page"] += 1
        except Exception as e:
            logger.error(f"Failed to fetch validation rules on page {params['page']}: {e}")
            break
    return all_validation_rules


def convert_list_to_dataframe(data):
    """
    Converts a list of validation rules data into a pandas DataFrame.
    Args:
        data (list): The list of validation rules data.
    Returns:
        pd.DataFrame: A pandas DataFrame containing the validation rules.
    Raises:
        Exception: If an error occurs during the DataFrame conversion.
    """
    try:
        df = pd.json_normalize(data, max_level=0)
        return df
    except Exception as e:
        logger.error(f"Failed to convert data to DataFrame: {e}")
        raise


def ingest_api_validation_rules(ti, n_days_ago=1):
    """
    Ingests validation rules from the Voucherify API and saves them to an S3 bucket.
    Args:
        ti (TaskInstance): The Airflow TaskInstance object.
        n_days_ago (int, optional): Number of days ago to fetch validation rules from.
        Defaults to 1.
    """
    # Dates
    PAST_DAY_ISO_FORMAT, TODAY_ISO_FORMAT = get_iso_date_range(n_days_ago=n_days_ago)
    # Variables
    voucherify_app_id = Variable.get("voucherify_app_id",
                                     deserialize_json=False)
    voucherify_secret_key = Variable.get("voucherify_secret_key",
                                         deserialize_json=False)
    BUCKET_NAME = "grover-eu-central-1-production-data-raw"
    ENTITY = 'validation_rules'
    S3_PREFIX = "voucherify"

    validation_rules = fetch_all_validation_rules(voucherify_app_id,
                                                  voucherify_secret_key,
                                                  PAST_DAY_ISO_FORMAT)
    pandas_df = convert_list_to_dataframe(validation_rules)
    s3_path = write_csv_file_s3(dataframe=pandas_df,
                                s3_bucket=BUCKET_NAME,
                                s3_prefix=S3_PREFIX,
                                entity=ENTITY,
                                date=TODAY_ISO_FORMAT,
                                filename=ENTITY)
    ti.xcom_push(key=f's3_file_path_{ENTITY}',
                 value=s3_path)
