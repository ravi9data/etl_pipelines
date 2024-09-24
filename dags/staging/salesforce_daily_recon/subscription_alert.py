import datetime
import logging

import airflow.providers.amazon.aws.hooks.redshift_sql as rd

from plugins.slack_utils import send_notification

logger = logging.getLogger(__name__)


def subscription_recon_alert(**kwargs):
    sql_query = """SELECT
        count(1)
        FROM stg_skyvia.test_subscription_payment__c a
        INNER JOIN stg_salesforce.subscription_payment__c b
        ON a.id = b.id
        WHERE true
        AND date(a.lastmodifieddate)<current_date -  1
        AND a.lastmodifieddate > b.lastmodifieddate
        AND a.status__c <> b.status__c
    """
    logging.info('Fetch data from redshift')

    rs = rd.RedshiftSQLHook(redshift_conn_id='redshift_default')
    conn = rs.get_conn()
    cr = conn.cursor()
    cr.execute(sql_query)
    row_count = cr.fetchone()[0]
    msg = f"Total Number of Updated Records of Subscription Payment : " \
          f"{datetime.datetime.now().strftime('%Y-%m-%d')} executed successfully" \
          f"\n" + str(row_count)

    send_notification(message=msg, slack_conn='slack')


def subscription_history_recon_alert(**kwargs):
    sql_query = """SELECT
        count(1)
        FROM stg_skyvia.test_subscription_payment_history  a
        left join   stg_salesforce.subscription_payment_history b
        on a.parentid = b.parentid
        where b.parentid is null
    """
    logging.info('Fetch data from redshift')

    rs = rd.RedshiftSQLHook(redshift_conn_id='redshift_default')
    conn = rs.get_conn()
    cr = conn.cursor()
    cr.execute(sql_query)
    row_count = cr.fetchone()[0]
    msg = f"Total Number of Inserted Records of Subscription Payment History: " \
          f"{datetime.datetime.now().strftime('%Y-%m-%d')} executed successfully" \
          f"\n" + str(row_count)

    send_notification(message=msg, slack_conn='slack')
