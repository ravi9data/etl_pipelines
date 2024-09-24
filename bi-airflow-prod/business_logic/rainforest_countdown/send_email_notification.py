import logging
import os
import smtplib
import ssl
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import numpy as np
import pandas as pd
from airflow import AirflowException
from airflow.models import Variable


def extract_db_data(sql_filepath):
    # number formatters
    def int_frmt(x):
        return '{:,}'.format(x)

    def float_frmt(x):
        return '{:,.2f}'.format(x) if x > 1e3 else '{:,.2f}'.format(x)

    frmt_map = {np.dtype('int64'): int_frmt, np.dtype('float64'): float_frmt}

    rs = rd.RedshiftSQLHook(redshift_conn_id='redshift_default')

    tbl_suffix = (datetime.today().replace(day=1) - timedelta(days=1)
                  ).replace(day=1).strftime('%Y%m') + '_mid_month '

    with open(sql_filepath, mode="r", encoding="utf-8") as file:
        sql_query = file.read().format(tbl_suffix=tbl_suffix)
        conn = rs.get_conn()
        df = pd.read_sql_query(sql_query, conn)
        if len(df.index) == 0:
            raise Exception("No Data available for extraction")
        conn.close()

        frmt = {col: frmt_map[df.dtypes[col]] for col in df.columns if
                df.dtypes[col] in frmt_map.keys()}

        return df.to_html(formatters=frmt, index=False)


def send_email_notification(email_recipient, email_subject):
    ctx = ssl.create_default_context()
    password = Variable.get('bi_gmail_pwd')
    sender = "bi@grover.com"
    receiver = email_recipient

    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

    email_message = """
                <html>
                    <head></head>
                    <body>
                        <p>
                            Hello ! <br><br>
                            Rainforest metrics - Weekly: <br>
                                {rainforest_metrics}<br><br>

                            Countdown metrics - Weekly: <br><br>
                                {countdown_metrics}<br><br>

                            Best regards,<br>
                            Grover
                        </p>
                    </body>
                </html>
            """

    rainforest_metrics = extract_db_data(os.path.join(__location__, "sql/rainforest.sql"))
    countdown_metrics = extract_db_data(os.path.join(__location__, "sql/countdown.sql"))

    try:
        logging.info('Sending Mail to : {}'.format(email_recipient))

        # Create the message
        message = MIMEMultipart("mixed")
        message["Subject"] = email_subject
        message["From"] = sender
        message["To"] = ", ".join(email_recipient)

        message.attach(MIMEText(email_message.format(rainforest_metrics=rainforest_metrics,
                                                     countdown_metrics=countdown_metrics),
                                "html"))

        # Connect with server and send the message
        with smtplib.SMTP_SSL("smtp.gmail.com", port=465, context=ctx,
                              local_hostname='airflow') as server:
            server.login(sender, password)
            # omit internal recipients from Mime to use Bcc.
            server.sendmail(sender, receiver, message.as_string())
    except Exception as e:
        logging.error('Error occurred:  {}'.format(str(e)))
        raise AirflowException
