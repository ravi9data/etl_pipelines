import logging

import gspread
import pandas as pd
from airflow.models import Variable

from plugins.utils.google_cloud import make_google_cloud_conn
from plugins.utils.redshift import get_alchemy_connection
from plugins.utils.sql import read_sql_file

logger = logging.getLogger(__name__)

REDSHIFT_CONN_ID = 'redshift'
GOOGLE_CLOUD_CONN = 'google_service_account_authorization'


def load_redshift_data_to_gsheet(sqlfile, var_gsheet_id, sheet_name, **kwargs):
    """
    Extract order data from Redshift and firstly load to a Pandas Dataframe. Then make the Google
    Cloud connection using our DENG google service account as stored in the Airflow variable. This
    has write access to the target CRM Google sheets. The data in the sheets are completely
    refreshed each time.
    """
    credentials_json = Variable.get(GOOGLE_CLOUD_CONN)
    gsheet_id = Variable.get(var_gsheet_id)
    logger.info('Extracting customer order data from Redshift')
    df = pd.read_sql(
                sql=read_sql_file(sqlfile),
                con=get_alchemy_connection(REDSHIFT_CONN_ID))
    logger.info('Loaded the data to a dataframe')

    try:
        logger.info('Attempting to open the target Googlesheet.')
        client = gspread.authorize(make_google_cloud_conn(
                        credentials_json,
                        list_scopes=['https://www.googleapis.com/auth/spreadsheets']))
        spreadsheet = client.open_by_key(gsheet_id)
        data = spreadsheet.worksheet(str(sheet_name))
    except Exception as err:
        logger.error('Failed to open the Googlesheet.')
        raise SystemExit(err)

    logger.info('Clearing worksheet before writing new records.')
    data.clear()
    logger.info('Writing the dataframe to the Googlesheet.')
    data.update([df.columns.values.tolist()] + df.values.tolist())
