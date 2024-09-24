import logging

from airflow.models import Variable

from plugins.s3_utils import (full_path_from_variable, get_s3_client,
                              get_s3_paginator)
from plugins.scopas_utils import full_path_from_threshold_check

logger = logging.getLogger(__name__)


def get_latest_report_file():
    config = Variable.get('scopas_rebuy_eu_config', deserialize_json=True)
    s3_bucket = config['s3_bucket']
    object_prefix = config['prefix']
    days_threshold = config['days_threshold']
    s3_objects = get_s3_paginator(
        s3_client=get_s3_client(),
        bucket_name=s3_bucket,
        prefix=object_prefix
        )

    scopas_date_prefix = config.get('backfill_date_prefix')
    if scopas_date_prefix:
        logging.info('getting date prefix from Airflow UI for backfilling')
        full_path = full_path_from_variable(s3_objects, s3_bucket, scopas_date_prefix)
        return full_path
    else:
        full_path = full_path_from_threshold_check(s3_objects, s3_bucket, days_threshold)
        return full_path
