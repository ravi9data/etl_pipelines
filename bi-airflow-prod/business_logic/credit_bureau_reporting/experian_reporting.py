import logging

from plugins.s3_utils import (current_path, read_file_object_s3,
                              write_csv_file_s3)
from plugins.slack_utils import send_notification
from plugins.utils.redshift_utils import (execute_redshift_query,
                                          redshift_conn,
                                          redshift_select_query_to_df,
                                          truncate_redshift_table)
from plugins.utils.sftp import transfer_to_sftp

logger = logging.getLogger(__name__)


# Variables
SLACK_CONN = "slack"
EXPERIAN_SFTP_CONN = "experian_nl_delinquency_report_prod"
REDSHIFT_CONN_ID = "redshift_default"
EXPERIAN_TABLE_NAME = "ods_data_sensitive.experian_reporting"
SFTP_FOLDER_NAME = "/to_xpn/"


# pylint: disable=W1203,W0718,R0913,R0914,R0915
def experian_reporting(bucket_name, query_path,
                       report_path, csv_filename,
                       rename_dict, columns_to_keep):
    """
    Fetches Experian reporting data from Redshift, exports it to S3 and SFTP.
    Parameters:
    bucket_name (str): Name of the S3 bucket.
    query_path (str): Path to the SQL query file on S3.
    report_path (str): Path to save the CSV report on S3.
    csv_filename (str): Filename for the CSV report.
    rename_dict (dict): Dictionary mapping old column names to new column names.
    columns_to_keep (list): List of column names to keep in the final DataFrame.
    """
    try:
        experian_reporting_query = read_file_object_s3(bucket_name, query_path)
        logger.info(f"Experian query: {experian_reporting_query}")
        conn, cursor = redshift_conn(REDSHIFT_CONN_ID)
        truncate_redshift_table(conn, cursor, EXPERIAN_TABLE_NAME)
        execute_redshift_query(conn, cursor, experian_reporting_query)
        select_query = f"SELECT * FROM {EXPERIAN_TABLE_NAME};"
        df = redshift_select_query_to_df(cursor, select_query)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
    # Transformations
    # Rename Columns according to Experian Requeriment
    logger.info("Renaming Columns according to Experian Requeriment.")
    df = df.rename(columns=rename_dict)
    logger.info(f"df******** {df.head()}")
    # Drop unnecessary columns
    logger.info("Dropping unnecessary Columns.")
    columns_to_drop = [col for col in df.columns if col not in columns_to_keep]
    logger.info(f"Columns to drop {columns_to_drop}")
    df = df.drop(columns=columns_to_drop)
    df = df[columns_to_keep]
    logger.info(f"df******** {df.head()}")

    # Saving csv file in S3 bucket
    current_price_data_path = current_path(f"s3://{bucket_name}{report_path}{csv_filename}")
    logger.info(f"Writing csv file to: {current_price_data_path}")
    write_csv_file_s3(df, current_price_data_path)

    # Saving csv file to SFTP
    logger.info("Writing csv file to SFTP")
    transfer_to_sftp(df, csv_filename, SFTP_FOLDER_NAME, EXPERIAN_SFTP_CONN)

    message = (f"*Experian Delinquency Report:* "
               f"Saved CSV to S3 bucket:\n {current_price_data_path}"
               f"\nExported the file to Experian SFTP: "
               f"`{SFTP_FOLDER_NAME}{csv_filename}` ")

    send_notification(message, SLACK_CONN)
