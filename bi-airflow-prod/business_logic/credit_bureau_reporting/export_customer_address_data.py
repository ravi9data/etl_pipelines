import datetime
import logging
import os

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import pandas as pd
from airflow.providers.sftp.hooks.sftp import SFTPHook

from plugins.slack_utils import send_notification

logger = logging.getLogger(__name__)

EXPERIAN_CS_PROD = "experian_normalizer"
SLACK_CONN = 'credit_bureau_reporting_notifications'


def export_ca_report():
    rs = rd.RedshiftSQLHook(redshift_conn_id="redshift_default")

    dt = datetime.datetime.now().strftime("%Y%m%d")

    export_file_name = f"grover_address_normalization_{dt}.xlsx"

    conn = rs.get_conn()
    sql_query = """SELECT
            DISTINCT
            customer_id AS id ,
            shipping_address_street,
            shipping_house_number,
            shipping_city AS city,
            shipping_zipcode AS zipcode,
            shipping_country AS country
        FROM
            ods_data_sensitive.credit_bureau_spain_shipaddress
        WHERE lastest_address_normalized = FALSE
        AND snapshot_date = current_date
        """
    df = pd.read_sql_query(sql_query, conn)

    logger.info('Exporting data to Excel :')

    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))
    local_filepath = os.path.join(__location__, export_file_name)

    df.to_excel(local_filepath,
                sheet_name='Sheet1',
                index=False)

    remote_filepath = f"/to_xpn/{export_file_name}"

    sftp_hook = SFTPHook(ssh_conn_id=EXPERIAN_CS_PROD)
    logger.info("opening conn")
    file_msg = f"from {local_filepath} to {remote_filepath}"
    logger.info("Starting to transfer file %s", file_msg)
    sftp_hook.store_file(remote_filepath, local_filepath)
    logger.info("closing conn")
    sftp_hook.close_conn()

    message = (f"*Customer Address Normalization:* "
               f"\nExported the file to Experian : "
               f"`{export_file_name}` "
               f"\nTotal Records processed: *{len(df.index)}*")

    send_notification(message, SLACK_CONN)
