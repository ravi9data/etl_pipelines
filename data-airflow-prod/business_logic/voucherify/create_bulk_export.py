import logging
import time
from io import StringIO

import numpy as np
import pandas as pd
import requests
from airflow.models import Variable

from plugins.voucherify_utils import (convert_columns_to_snake_case,
                                      get_iso_date_range, write_csv_file_s3)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_column_recurring(value):
    """
    Transforms a value based on specific conditions:
    1. If the value is NaN, 'true', or 'false',
    it is returned as is.
    2. If the value is a string that can be converted to a number,
    it returns 'true'.
    3. Otherwise, it returns NaN.
    Parameters:
    value (any): The input value to be transformed.
    Returns:
    any: The transformed value based on the specified conditions.
    """
    if pd.isna(value) or value == 'true' or value == 'false':
        return value
    elif isinstance(value, str):
        try:
            float(value)
            return 'true'
        except ValueError:
            return np.nan
    else:
        return np.nan


def get_columns_to_keep(entity):
    """
    Returns a list of columns to keep based on the provided entity type.
    Parameters:
    entity (str): The type of entity. Expected values are "voucher"
    or "redemption".
    Returns:
    list: A list of column names corresponding to the specified entity type.
          If the entity type is not recognized, an empty list is returned.
    """
    columns = {
        'redemption': [
            'id', 'object', 'date', 'voucher_code', 'campaign',
            'promotion_tier_id', 'customer_id', 'customer_source_id',
            'customer_name', 'tracking_id', 'order_id', 'order_amount',
            'gift_amount', 'loyalty_points', 'result', 'failure_code',
            'failure_message', 'amount_of_active_subscriptions',
            'declined_subscriptions', 'ended_subscriptions', 'items_total',
            'lowest_subscription_plan', 'processing_subscriptions', 'store_code',
            'store_type', 'user_type'],
        'voucher': [
            'code', 'voucher_type', 'value', 'discount_type', 'campaign',
            'category', 'start_date', 'expiration_date', 'gift_balance',
            'loyalty_card_balance', 'redemption_limit', 'redemption_count',
            'active', 'qr_code', 'barcode', 'id', 'is_referral_code',
            'creation_date', 'last_update_date', 'discount_amount_limit',
            'campaign_id', 'additional_info', 'customer_id', 'discount_unit_type',
            'discount_unit_effect', 'customer_source_id', 'validation_rules_id',
            'allocation', 'asset', 'locale', 'order', 'payment', 'recurring',
            'request',
            ]
    }

    return columns.get(entity, [])


def create_request_body_filter(date_filter, entity):
    """
    Create the request body for filtering data based on entity
    type and date filter.
    Parameters:
    date_filter (str): The date filter to apply for created_at and updated_at fields.
    entity (str): The type of entity for which to create the request body.
    Should be 'voucher' or 'redemption'.
    Returns:
    dict: The request body for filtering data.
    Raises:
    ValueError: If the provided entity is not valid.
    """
    # Define constants for dictionary keys
    VOUCHER = "voucher"
    REDEMPTION = "redemption"

    # Fields for each entity
    ASSETS_FIELDS = {
        VOUCHER: [
            'code', 'voucher_type', 'value', 'discount_type', 'campaign', 'category',
            'start_date', 'expiration_date', 'gift_balance', 'loyalty_balance',
            'redemption_quantity', 'redemption_count', 'active', 'qr_code', 'bar_code',
            'metadata', 'id', 'is_referral_code', 'created_at', 'updated_at',
            'validity_timeframe_interval', 'validity_timeframe_duration', 'validity_day_of_week',
            'discount_amount_limit', 'campaign_id', 'additional_info', 'customer_id',
            'discount_unit_type', 'discount_unit_effect', 'customer_source_id',
            'validation_rules_id'
        ],
        REDEMPTION: [
            'id', 'object', 'date', 'voucher_code', 'campaign', 'promotion_tier_id',
            'customer_id', 'customer_source_id', 'customer_name', 'tracking_id', 'order_id',
            'order_amount', 'gift_amount', 'loyalty_points', 'result', 'failure_code',
            'failure_message', 'metadata'
        ]
    }

    # Common filter structure
    common_filters = {
        "junction": "OR",
        "created_at": {
            "conditions": {
                "$after": date_filter
            }
        },
        "updated_at": {
            "conditions": {
                "$after": date_filter
            }
        }
    }

    # Filters for each entity
    ASSETS_FILTERS = {
        VOUCHER: common_filters,
        REDEMPTION: common_filters
    }

    # Order for each entity
    ASSETS_ORDER = {
        VOUCHER: "-updated_at",
        REDEMPTION: "-created_at"
    }

    # Validate entity
    if entity not in ASSETS_FIELDS:
        raise ValueError(f"Invalid entity '{entity}'. Must be 'voucher' or 'redemption'.")

    # Construct the request body
    request_body = {
        "exported_object": entity,
        "parameters": {
            "order": ASSETS_ORDER[entity],
            "fields": ASSETS_FIELDS[entity],
            "filters": ASSETS_FILTERS[entity],
        }
    }

    return request_body


def create_export_action(date_filter,
                         entity,
                         voucherify_app_id,
                         voucherify_secret_key):
    """
    Create an export action by sending a request to the Voucherify API.
    Args:
        date_filter (dict): Dictionary containing date filter parameters.
        entity (str): The entity type for the export.
        voucherify_app_id (str): Voucherify application ID.
        voucherify_secret_key (str): Voucherify secret key.
    Returns:
        dict: Response JSON from the Voucherify API.
    Raises:
        requests.exceptions.RequestException: If the request fails.
    """
    # Constants
    ENDPOINT_URL = "https://api.voucherify.io/v1/exports"
    TIMEOUT = 180
    request_body = create_request_body_filter(date_filter,
                                              entity)
    headers = {
        "X-App-Id": voucherify_app_id,
        "X-App-Token": voucherify_secret_key,
        'X-Voucherify-Channel': 'Python-SDK',
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(ENDPOINT_URL,
                                 headers=headers,
                                 json=request_body,
                                 timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exception:
        logger.error(f"Request failed: {exception}")
        raise


def check_export_status(export_id, voucherify_app_id, voucherify_secret_key):
    """
    Check the status of an export action until it
    is done or the retry limit is reached.
    Args:
        export_id (str): The ID of the export.
        VOUCHERIFY_APP_ID (str): Voucherify application ID.
        VOUCHERIFY_SECRET_KEY (str): Voucherify secret key.
    Returns:
        str: The URL of the completed export or None if not found.
    Raises:
        requests.exceptions.RequestException: If the request fails.
    """
    # Constants
    TIMEOUT = 180
    RETRY_LIMIT = 60
    RETRY_INTERVAL = 3

    # Define headers
    headers = {
        "X-App-Id": voucherify_app_id,
        "X-App-Token": voucherify_secret_key,
        'X-Voucherify-Channel': 'Python-SDK',
        'Content-Type': 'application/json'
    }

    try:
        for attempt in range(RETRY_LIMIT):
            # Define the URL for checking the export status
            status_url = f"https://api.voucherify.io/v1/exports/{export_id}"

            # Send the GET request to check the status
            status_response = requests.get(status_url,
                                           headers=headers,
                                           timeout=TIMEOUT)
            status_response.raise_for_status()

            # Get the status from the response
            status = status_response.json().get('status')
            if status == "DONE":
                # Return the result URL if the export is done
                return status_response.json().get('result', {}).get('url', None)
            else:
                # Log the current status and sleep for 2 seconds before checking again
                logger.info(f"Export status: {status}."
                            f"Checking again in {RETRY_INTERVAL} seconds.")
                time.sleep(RETRY_INTERVAL)

        logger.error("Export did not complete within the retry limit.")
        return None

    except requests.exceptions.RequestException as exception:
        # Log the exception
        logger.error(f"Request failed: {exception}")
        # Re-raise the exception to be handled by the caller
        raise


def download_export_from_url(url, voucherify_app_id, voucherify_secret_key):
    """
    Downloads a CSV file from a given URL and reads it into a pandas DataFrame.
    Args:
        url (str): The URL of the CSV file to download.
    Returns:
        pd.DataFrame: The downloaded CSV data as a pandas DataFrame if successful.
        str: An error message if the request fails.
    Raises:
        requests.exceptions.RequestException: If there's an issue with the HTTP request.
    """
    TIMEOUT = 180
    headers = {
        "X-App-Id": voucherify_app_id,
        "X-App-Token": voucherify_secret_key
    }

    try:
        response = requests.request("GET",
                                    url,
                                    headers=headers,
                                    timeout=TIMEOUT)
        response.raise_for_status()  # Raise an exception for HTTP errors
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)
        return df
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading file: {e}")
        raise


def ingest_from_export_api(entity,
                           ti,
                           n_days_ago=1):
    """
    Ingests data from the Voucherify export API.
    Args:
        entity (str): The entity for which data is being ingested.
        ti (TaskInstance): The Airflow TaskInstance object.
        n_days_ago (int, optional): The number of days ago from which to ingest data.
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
    S3_PREFIX = "voucherify"

    export_response = create_export_action(PAST_DAY_ISO_FORMAT,
                                           entity,
                                           voucherify_app_id,
                                           voucherify_secret_key)
    export_id = export_response.get('id', None)
    export_url = check_export_status(export_id,
                                     voucherify_app_id,
                                     voucherify_secret_key)
    pandas_df = download_export_from_url(export_url,
                                         voucherify_app_id,
                                         voucherify_secret_key)
    renamed_col_df = convert_columns_to_snake_case(pandas_df)
    columns_to_keep = get_columns_to_keep(entity)
    columns_to_drop = [col for col in renamed_col_df.columns if col not in columns_to_keep]
    clean_df = renamed_col_df.drop(columns=columns_to_drop)
    if entity == "voucher":
        clean_df['recurring'] = clean_df['recurring'].apply(transform_column_recurring)
    s3_path = write_csv_file_s3(dataframe=clean_df,
                                s3_bucket=BUCKET_NAME,
                                s3_prefix=S3_PREFIX,
                                entity=entity,
                                date=TODAY_ISO_FORMAT,
                                filename=entity)
    ti.xcom_push(key=f's3_file_path_{entity}',
                 value=s3_path)
