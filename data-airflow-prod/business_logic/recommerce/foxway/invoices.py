import logging
from datetime import date, datetime, timedelta

from airflow.models import Variable
from awswrangler.s3 import to_csv

from business_logic.recommerce.foxway.config import (invoices_col_names,
                                                     invoices_use_cols)
from plugins.s3_utils import get_s3_full_path
from plugins.utils.recommerce import (excel_attachment_to_dataframe,
                                      transform_dataframe)

logger = logging.getLogger(__name__)

GMAIL_ADDRESS_VAR_NAME = 'deng_google_account'
APP_PASSWORD_VAR_NAME = 'gmail_app_password'


def extract_email_attachment_to_s3(dag_config, **context):
    """
    Builds the email_date and report_date if not present in the Variables.
    Searches for the email excel attachment and loads to a Pandas DataFrame.
    The data is transformed to add the partition columns, and written to
    S3 / Glue.
    """
    config = Variable.get(dag_config, deserialize_json=True)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    glue_database = config['glue_database']
    glue_table = config['glue_table']
    email_subject = config['email_subject']
    sheet_name = config['sheet_name']
    username = Variable.get(GMAIL_ADDRESS_VAR_NAME)
    password = Variable.get(APP_PASSWORD_VAR_NAME)

    today = date.today()
    prev_month = (today - timedelta(days=today.day)).strftime("%m-%Y")
    email_subject = email_subject+' '+prev_month
    logger.info(f'Target email subject: `{email_subject}`')

    email_date = config.get('email_date', None)
    if isinstance(email_date, str):
        logger.info(f'Found email_date {email_date} from config, converting string to date object')
        email_date = datetime.strptime(email_date, '%Y-%m-%d').date()
        email_date_gt = None
        email_date_lt = None

    if not email_date:
        email_date_gt = today.replace(day=1)
        email_date_lt = today.replace(day=7)
        logger.info(f'No specific date provided, searching for email `{email_subject}` for dates '
                    f'between {email_date_gt} and {email_date_lt}')

    logger.info('Pushing report_date to XCOM')
    default_reporting_date = today.replace(day=6).strftime('%Y-%m-%d')
    reporting_date = config.get('reporting_date', default_reporting_date)
    task_instance = context['ti']
    task_instance.xcom_push(key="reporting_date", value=reporting_date)

    invoices_data = excel_attachment_to_dataframe(dag_config=dag_config,
                                                  username=username,
                                                  password=password,
                                                  subject=email_subject,
                                                  sheet_name=sheet_name,
                                                  email_date=email_date,
                                                  email_date_gt=email_date_gt,
                                                  email_date_lt=email_date_lt,
                                                  header=None,
                                                  names=invoices_col_names,
                                                  usecols=invoices_use_cols,
                                                  skiprows=3)

    logger.info(f'Number of records retrieved: {invoices_data.shape[0]}')
    logger.info('Removing last 4 rows which are not valid data')
    invoices_data = invoices_data.iloc[:-4]
    logger.info(f'Number of records after cleanup: {invoices_data.shape[0]}')

    logger.info(f'Adding column `reporting_date` to the DataFrame: {reporting_date}')
    invoices_data.insert(loc=0, column='reporting_date', value=reporting_date)

    logger.info('Transforming dataframe for writing to S3')
    invoices_data, _ = transform_dataframe(input_df=invoices_data,
                                           partition_cols=['year', 'month', 'day'])

    logger.info('Writing dataframe to S3')
    s3_writer = to_csv(df=invoices_data,
                       path=s3_destination_path,
                       sep='|',
                       index=False,
                       dataset=True,
                       mode='overwrite_partitions',
                       database=glue_database,
                       table=glue_table,
                       partition_cols=['year', 'month', 'day', 'batch_id'])

    logger.info('Pushing partition values to XCOM')
    for partition in s3_writer['partitions_values'].values():
        task_instance.xcom_push(key="year", value=partition[0])
        task_instance.xcom_push(key="month", value=partition[1])
        task_instance.xcom_push(key="day", value=partition[2])
        task_instance.xcom_push(key="batch_id", value=partition[3])

    return s3_writer['paths'][0]
