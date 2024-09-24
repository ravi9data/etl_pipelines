import logging

# import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import pandas as pd
from gspread.exceptions import APIError
from gspread_dataframe import set_with_dataframe

from plugins.utils.gsheet import get_gspread_client
from plugins.utils.retry import retry

logger = logging.getLogger(__name__)


@retry(APIError, tries=5, delay=10, backoff=2, logger=logger)
def export_dataframe_to_gsheet(data: pd.DataFrame, filename: str, gdrive_folder_id: str):
    gc = get_gspread_client('google_cloud_default')

    if gdrive_folder_id:
        for spreadsheet in gc.list_spreadsheet_files(folder_id=gdrive_folder_id):
            if spreadsheet['name'] == filename:
                logging.info(f"Deleting existing sheet with same name : {filename}")
                gc.del_spreadsheet(spreadsheet['id'])

    gc.create(filename, folder_id=gdrive_folder_id)
    sheet = gc.open(filename).sheet1
    set_with_dataframe(sheet, data)


def extract_supplier_report_weekly(conn, filename, sql_query, gdrive_folder_id: str):
    # rs = rd.RedshiftSQLHook(redshift_conn_id='redshift_default')
    # conn = rs.get_conn()

    df = pd.read_sql_query(sql_query, conn)
    if len(df.index) > 0:
        export_dataframe_to_gsheet(df, filename, gdrive_folder_id)
    else:
        logger.info("No Data available for extraction")

    return None
