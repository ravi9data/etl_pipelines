import io
import logging

import boto3
import requests
from airflow import AirflowException
from airflow.models import Variable

logger = logging.getLogger(__name__)


def download_csv_to_s3(csv_url, file_path):
    s3_bucket = Variable.get('s3_bucket_curated_bi',
                             default_var='grover-eu-central-1-production-data-bi-curated')

    with requests.Session() as s:
        download = s.get(csv_url)
        if download.status_code == 200:
            logging.info(f"file downloaded successfully from URL : {csv_url}")
            file_content = io.BytesIO(download.content)
            s3 = boto3.client('s3')
            s3.upload_fileobj(file_content, s3_bucket, file_path)
            logging.info(f"Uploaded file to s3://{s3_bucket}/{file_path}")
        else:
            logging.error(f"Failed to download the file from URL : {csv_url}")
            raise AirflowException
