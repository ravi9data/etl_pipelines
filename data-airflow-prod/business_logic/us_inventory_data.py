import datetime
import logging
import os
import re

import awswrangler as wr
from airflow.models import Variable

from plugins import s3_utils

DESTINATION_PREFIX = "us/inventory"

main_log = "business_logic.us_inventory_data"
logger = logging.getLogger(__name__)


def get_prefix():
    base_prefix = 'Standard OH Inventory Detail_CSV'
    default_date = str(datetime.datetime.utcnow().date())
    extraction_date = Variable.get('us_inventory_date', default_var=default_date)
    return f'{base_prefix}_{extraction_date}'


def get_latest_report_file():
    s3_bucket = Variable.get('us_inventory_s3_bucket', default_var='ups-update-bucket-production')
    prefix = get_prefix()
    logger.info(f'Fetching keys from prefix s3://{s3_bucket}/{prefix}')
    s3_objects = s3_utils.get_s3_paginator(
        s3_client=s3_utils.get_s3_client(),
        bucket_name=s3_bucket,
        prefix=get_prefix())
    logger.debug(s3_objects)
    filter_keys = [i for i in s3_objects if i["Key"].endswith("json")]
    latest_key, latest_date = s3_utils.get_last_modified_object(filter_keys)
    logger.info(latest_key)
    return {
        's3_bucket': s3_bucket,
        's3_key': latest_key,
        's3_full_path': os.path.join('s3://', s3_bucket, latest_key)
    }


def copy_to_s3(**context):
    latest_file = context['ti'].xcom_pull(key='return_value', task_ids='get_latest_report_file')
    logger.info(latest_file)
    raw_data = wr.s3.read_json(path=latest_file["s3_full_path"], use_threads=True)
    key_year = re.findall(r"\d{4}", latest_file["s3_key"])[0]
    key_month = re.findall(r"\d{2}", latest_file["s3_key"])[2]
    key_day = re.findall(r"\d{2}", latest_file["s3_key"])[3]
    s3_key_extension_format = latest_file["s3_key"].replace("json", "csv").replace(" ", "-")
    s3_destination_bucket = Variable.get('us_inventory_s3_bucket_destination')
    s3_destination_key = f's3://{s3_destination_bucket}/{DESTINATION_PREFIX}/year={key_year}/month={key_month}\
/day={key_day}/{s3_key_extension_format}'
    result = wr.s3.to_csv(df=raw_data, path=s3_destination_key, index=False)
    logger.info(result)
    return s3_destination_key


def get_datetime_from_filename(**context):
    latest_file = context['ti'].xcom_pull(key='return_value', task_ids='get_latest_report_file')
    s3_key = latest_file["s3_key"]
    date_time_extract = re.findall(r"\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}", s3_key)[0]
    date_time_format = date_time_extract[:10] + " " + date_time_extract[10+1:]
    date_time_final = date_time_format[:11] + date_time_format[11:].replace("-", ":")
    return date_time_final
