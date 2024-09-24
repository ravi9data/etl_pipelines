import datetime
import logging
import os

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import pandas as pd
from airflow.providers.sftp.hooks.sftp import SFTPHook

from plugins.slack_utils import send_notification

logger = logging.getLogger(__name__)

EXPERIAN_CS_PROD = "experian_cs_prod_sftp"
SLACK_CONN = 'credit_bureau_reporting_notifications'


def export_cb_report():
    rs = rd.RedshiftSQLHook(redshift_conn_id="redshift_default")

    dt = datetime.datetime.now().strftime("%Y%m%d")

    export_file_name = f"07106-{dt}.txt"

    conn = rs.get_conn()
    sql_query = "SELECT * FROM dm_risk.v_credit_bureau_reporting"
    df = pd.read_sql_query(sql_query, conn)

    # replace characters which are incompatible with cp1252 encoding
    df['rec'] = df['rec'].str.replace(u'\u015E', 'S')
    df['rec'] = df['rec'].str.replace(u'\u015F', 's')
    df['rec'] = df['rec'].str.replace('"', '')

    logger.info('Exporting data to CSV :')

    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))
    local_filepath = os.path.join(__location__, export_file_name)
    df.to_csv(local_filepath,
              index=False,
              header=False,
              encoding="cp1252",
              line_terminator="\r\n")

    remote_filepath = f"/Entrada/{export_file_name}"

    sftp_hook = SFTPHook(ssh_conn_id=EXPERIAN_CS_PROD)
    logger.info("opening conn")
    file_msg = f"from {local_filepath} to {remote_filepath}"
    logger.info("Starting to transfer file %s", file_msg)
    sftp_hook.store_file(remote_filepath, local_filepath)
    logger.info("closing conn")
    sftp_hook.close_conn()

    message = (f"*Credit Bureau reporting:* "
               f"\nExported the file to Experian : "
               f"`{export_file_name}` "
               f"\nTotal Records processed: *{len(df.index)}*")

    send_notification(message, SLACK_CONN)
