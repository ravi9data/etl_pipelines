import datetime
import json
import logging
import uuid

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from plugins.dag_utils import on_failure_callback

DAG_ID = 'example_aws_dag'

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 9, 19),
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=5),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=10)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='A simple DAG to interact with S3',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=['example', DAG_ID, 'aws']
)


def list_s3_bucket():
    """
    Function that list all the objects in a S3 bucket.
    By default the AWS base hook use aws_default connection id.
    If you plan to benefit of EC2 instance profile, e.g. when running from K8S workers,
    leave the credentials empty.
    """
    s3_bucket = Variable.get('s3_bucket_example')
    client = AwsBaseHook(client_type='s3').get_conn()

    paginator = client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=s3_bucket,
        PaginationConfig={
            'PageSize': 1000,
        }
    )

    objects = response_iterator.build_full_result().get('Contents')
    objects_len = 0 if objects is None else len(objects)
    logger.info(f'Total amount of objects in {s3_bucket} is: {objects_len}')


def get_partition_path():
    today = datetime.datetime.utcnow().date()
    _year = str(today.year)
    _month = '{:02d}'.format(today.month)
    _day = '{:02d}'.format(today.day)

    return f'year={_year}/month={_month}/day={_day}'


def write_object_to_s3():
    """
    Function that write 2 objects to S3.
    The s3 bucket name where to write must be set in a variable called s3_bucket_example
    """
    s3_bucket = Variable.get('s3_bucket_example')
    hook = S3Hook()
    batch_id = str(uuid.uuid4())
    partition_path = get_partition_path()
    key = f'airflow/{partition_path}/example_{batch_id}.json'
    key_compressed = f'airflow/{partition_path}/example_{batch_id}.json.gz'
    payload = [{'name': 'just test from Airflow',
               'created_at': datetime.datetime.utcnow()},
               {'name': 'just another test from Airflow',
                'created_at': datetime.datetime.utcnow()}
               ]
    json_string = json.dumps(payload, default=str, indent=4)

    # write file
    hook.load_bytes(bucket_name=s3_bucket,
                    key=key,
                    bytes_data=json_string.encode(),
                    replace=True)
    logger.info(f'{key} written to {s3_bucket}')

    # write compressed file
    hook.load_string(bucket_name=s3_bucket,
                     key=key_compressed,
                     string_data=json_string,
                     compression='gzip', replace=True)
    logger.info(f'{key_compressed} written to {s3_bucket}')


list_s3_bucket_task = PythonOperator(
    dag=dag,
    task_id='list_s3_bucket',
    python_callable=list_s3_bucket
)

write_object_to_s3_task = PythonOperator(
    dag=dag,
    task_id='write_object_to_s3',
    python_callable=write_object_to_s3
)

write_object_to_s3_task >> list_s3_bucket_task
