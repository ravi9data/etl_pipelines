import logging
from datetime import date, datetime

import pandas as pd
from airflow.models import Variable
from awswrangler.s3 import to_csv

from plugins.s3_utils import get_s3_full_path
from plugins.utils.recommerce import (excel_attachment_to_dataframe,
                                      transform_dataframe)

logger = logging.getLogger(__name__)

GMAIL_ADDRESS_VAR_NAME = 'deng_google_account'
APP_PASSWORD_VAR_NAME = 'gmail_app_password'


def extract_email_attachment_to_s3(dag_config, **context):
    """
    The email from Foxway arrives daily to DENG gmail `dataengsys@grover.com`.
    The target data is inside the excel attachment, which is then extracted,
    and loaded to a Pandas Dataframe.

    The excel sheet data is contained from columns A:R
    """
    config = Variable.get(dag_config, deserialize_json=True)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    glue_table = config['glue_table']

    email_subject = config['email_subject']
    sheet_name = config['sheet_name']
    attachment_fname = config['attachment_fname']
    sent_from = config.get('sent_from')
    username = Variable.get(GMAIL_ADDRESS_VAR_NAME)
    password = Variable.get(APP_PASSWORD_VAR_NAME)

    email_date = config.get('email_date', date.today())
    if isinstance(email_date, str):
        logger.info(f'Found email_date {email_date} from config, converting string to date object')
        email_date = datetime.strptime(email_date, '%Y-%m-%d').date()

    logger.info('Pushing report dates to XCOM')
    report_date = config.get('report_date', email_date.strftime('%Y-%m-%d'))
    task_instance = context['ti']
    task_instance.xcom_push(key="report_date", value=report_date)

    foxway_data = excel_attachment_to_dataframe(dag_config=dag_config,
                                                username=username,
                                                password=password,
                                                subject=email_subject,
                                                attachment_fname=attachment_fname,
                                                sheet_name=sheet_name,
                                                email_date=email_date,
                                                sent_from=sent_from,
                                                usecols="A:R")
    if foxway_data is None:
        return 'slack_message_failure'

    logger.info(f'Number of records retrieved: {foxway_data.shape[0]}')
    logger.info('Transforming date columns format')
    for col in ['Arrival date', 'Received date', 'Shipped Date', 'Payment date']:
        foxway_data[col] = pd.to_datetime(arg=foxway_data[col],
                                          infer_datetime_format=True,
                                          errors='coerce')
    logger.info(f'Number of records kept: {foxway_data.shape[0]}')
    logger.info('Transforming dataframe for writing to S3')
    foxway_data, batch_id = transform_dataframe(input_df=foxway_data,
                                                partition_cols=['year', 'month', 'day'],
                                                foxway_masterliste=True)
    logger.info('Writing dataframe to S3')
    s3_writer = to_csv(df=foxway_data,
                       path=s3_destination_path,
                       sep='|',
                       index=False,
                       dataset=True,
                       mode='append',
                       database=glue_database,
                       table=glue_table,
                       partition_cols=['year', 'month', 'day'])

    logger.info('Pushing batch_id, partition values and s3 output path to XCOM')
    task_instance.xcom_push(key="batch_id", value=batch_id)
    task_instance.xcom_push(key="s3_written_path", value=s3_writer['paths'][0])
    for partition in s3_writer['partitions_values'].values():
        task_instance.xcom_push(key="year", value=partition[0])
        task_instance.xcom_push(key="month", value=partition[1])
        task_instance.xcom_push(key="day", value=partition[2])

    return 'redshift.stage_data'
