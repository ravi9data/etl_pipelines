import logging

from airflow.exceptions import AirflowException
from airflow.models import Variable

logger = logging.getLogger(__name__)


def calculate_data_percentage(written_records_s3, orders_number):
    return (written_records_s3 / orders_number) * 100


def check_records_percentage(**kwargs):
    """
    This function help up to calculcate the percentage of data we are writing to redshift
    using three variables:
    - **written_records_s3**: is the length of the data we wrote in s3 as parquet files
    - **orders_number**: the number of orders we applied the data preparation on
    - **mr_record_percent_threshold**: Airflow variable in which we specify the percentage threshold

    If the percentage we are writing in RedShift exceed the threshold we don't do anything
    Else: we raise an Exception the DAG stops and we send slack notification
    """
    mr_record_percent_threshold = float(Variable.get('mr_record_percent_threshold'))
    task_instance = kwargs['ti']
    written_records_s3 = task_instance.xcom_pull(
        key='written_records', task_ids='prepare_manual_review_data')
    orders_number = task_instance.xcom_pull(
        key='orders_number', task_ids='prepare_manual_review_data')
    logger.info(f'Number of orders we extracted from Redshift {orders_number}')
    logger.info(f'Number of records written in s3 {written_records_s3}')
    data_percentage_to_write = calculate_data_percentage(written_records_s3, orders_number)
    if data_percentage_to_write < mr_record_percent_threshold:
        logger.error(f'*Error: we are trying to write {data_percentage_to_write}% which is lower\
            than {mr_record_percent_threshold}% our threshold')
        raise AirflowException(f'*Error: we are trying to write {data_percentage_to_write}% which is lower\
            than {mr_record_percent_threshold}% our threshold')
