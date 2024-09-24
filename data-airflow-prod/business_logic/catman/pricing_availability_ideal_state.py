import logging

import gspread
import pandas as pd
from airflow.models import Variable
from awswrangler.s3 import to_parquet

from plugins.s3_utils import get_s3_full_path
from plugins.utils.google_cloud import make_google_cloud_conn

logger = logging.getLogger(__name__)

CONFIG = 'pricing_availability_ideal_state_config'
GOOGLE_CLOUD_CONN = 'google_service_account_authorization'


def get_data_from_spreadsheet(**kwargs):
    """
    Make the Google Cloud connection using our DENG google service account as stored in the Airflow
    variable. This has access to the Catman Pricing Google sheet. The sheet data is transformed to
    boolean values and the file is overwritten in S3, as the source data is fully refreshed each
    time by the Catman team.
    """
    task_instance = kwargs['ti']
    config = Variable.get(CONFIG, deserialize_json=True)
    credentials_json = Variable.get(GOOGLE_CLOUD_CONN)
    s3_destination_bucket = config['s3_destination_bucket']
    s3_bucket_prefix = config['s3_bucket_prefix']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    gsheet_id = config['gsheet_id']

    try:
        logger.info('Attempting to open the Google sheet.')
        client = gspread.authorize(make_google_cloud_conn(
            credentials_json,
            list_scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']))
        worksheet = client.open_by_key(gsheet_id).sheet1
    except Exception as err:
        logger.error('Failed to open the Google sheet.')
        raise SystemExit(err)

    logger.info('Writing sheet data to a Dataframe.')
    df = pd.DataFrame(worksheet.get_all_records())
    df = df.replace({'TRUE': True, 'FALSE': False})

    s3_written_data = to_parquet(
                        df=df,
                        path=s3_destination_path,
                        index=False,
                        dataset=True,
                        mode='overwrite')

    task_instance.xcom_push(key='s3_outfile', value=s3_written_data['paths'][0])
