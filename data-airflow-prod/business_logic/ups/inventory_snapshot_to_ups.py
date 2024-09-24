import datetime
import logging

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import pandas as pd
from airflow import AirflowException
from airflow.models import Variable

from plugins.utils.send_email import send_email
from plugins.utils.sql import read_sql_file

CONFIG = 'send_inventory_snapshot_to_ups_config'

logger = logging.getLogger(__name__)


def send_inventory_snapshot_on_weekdays():
    """
    To send inventory snapshot to ups on weekdays
    """
    today_time = datetime.datetime.now()

    try:
        inventory_snapshot_config = Variable.get(CONFIG, deserialize_json=True)
        sender_email = inventory_snapshot_config['sender_email']
        sender_email_pwd = inventory_snapshot_config['sender_email_pwd']
        email_recipient = inventory_snapshot_config['email_recipient']
        cc_email_list = inventory_snapshot_config['cc_email_list']
        attachment_prefix = inventory_snapshot_config['attachment_prefix']
        email_subject_prefix = inventory_snapshot_config['email_subject_prefix']
        internal_recipients = inventory_snapshot_config['internal_recipients']

        redshift_conn = rd.RedshiftSQLHook(redshift_conn_id='redshift_default')
        conn = redshift_conn.get_conn()
        query = read_sql_file(
            './dags/ups/sql/inventory_snapshot.sql')
        logging.info('Extracting assets for reconciliation from stg_salesforce.asset table.')
        df = pd.read_sql_query(query, conn)
        conn.close()

        email_subject = '{} ({})'.format(email_subject_prefix,
                                         today_time.strftime("%d_%m_%y"))
        filename = '{0}-{1}.xlsx'.format(attachment_prefix,
                                         today_time.strftime("%d-%m-%Y-%H-%M-%S"))
        email_message = """
        <html>
        <p>Hi all,</p>
        <p>Here's the list of assets for the Reconciliation process.</p>
        <p>Best regards, <br>
        Grover.</p>
        </html>
        """
        logging.info('loading data into excel file : {}'.format(filename))
        df.to_excel(filename, engine='xlsxwriter')
        partner = 'ups'
        logging.info('Sending Data to partner : {}'.format(partner))
        send_email(sender_email,
                   sender_email_pwd,
                   email_recipient,
                   cc_email_list,
                   internal_recipients,
                   email_subject,
                   email_message,
                   filename)

    except Exception as e:
        logging.error('Error Occurred : {}'.format(e))
        raise AirflowException
