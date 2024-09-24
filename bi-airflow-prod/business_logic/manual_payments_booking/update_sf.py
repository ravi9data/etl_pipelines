import datetime
import logging

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import pandas as pd

from plugins.slack_utils import send_notification
from plugins.utils.salesforce import sf_bulk_update

logger = logging.getLogger(__name__)


def update_payment_booking_data(table_name, **kwargs):
    sql_query = f"""SELECT
            pbos.id AS Id,
            paid_date AS date_paid__c,
            paid_amount as amount_paid__c,
            paid_reason AS reason_paid__c,
            status AS status__c
        FROM {table_name} pbos
    """
    logging.info('Fetch data from redshift')

    rs = rd.RedshiftSQLHook(redshift_conn_id='redshift_default')
    conn = rs.get_conn()
    df = pd.read_sql_query(sql_query, conn)

    status = sf_bulk_update(data=df,
                            sf_conn_id='sf_prod',
                            object_name='subscription_payment__c',
                            batch_size=20)

    msg = f"Manual payment for reporting_date : " \
          f"{datetime.datetime.now().strftime('%Y-%m-%d')} executed successfully" \
          f"\n{status}"

    send_notification(message=msg, slack_conn='slack')
