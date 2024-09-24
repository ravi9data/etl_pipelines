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


def import_normalized_addresses():

    dt = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")

    file_name = f"grover_address_normalization_processed_{dt}.csv"

    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))
    local_filepath = os.path.join(__location__, file_name)
    remote_filepath = f"/from_xpn/{file_name}"

    sftp_hook = SFTPHook(ssh_conn_id=EXPERIAN_CS_PROD)
    logger.info("opening conn")
    file_msg = f"from {local_filepath} to {remote_filepath}"
    logger.info("Starting to transfer file %s", file_msg)
    sftp_hook.retrieve_file(remote_filepath, local_filepath)
    logger.info("closing conn")
    sftp_hook.close_conn()

    df = pd.read_csv(local_filepath, sep='|',
                     encoding='cp1252')

    conn = rd.RedshiftSQLHook('redshift_default').get_sqlalchemy_engine()
    df.to_sql(con=conn, method='multi', chunksize=5000, schema='dm_risk',
              name='tmp_grover_normalization_address', index=False, if_exists='replace')

    sql_query = """INSERT INTO dm_risk.grover_normalization_address
        SELECT
            codero_usuario,
            codigoc_norm,
            zipcode AS codigo_postal,
            codm_norm,
            codpostal_norm,
            codp_norm,
            shipping_address_street as direccion,
            id AS id1,
            localidad_norm,
            city AS municipio,
            municipio_norm,
            nexo_norm,
            shipping_house_number AS numero,
            numero_norm,
            particulas_norm,
            provincia_norm,
            resto_norm,
            tipo_via_norm,
            via_norm,
            CURRENT_DATE AS updated_date
        FROM
            dm_risk.tmp_grover_normalization_address gna"""

    conn.execute(sql_query)

    message = (f"*Customer Address Normalization:* "
               f"\nProcessed the file from Experian : "
               f"`{file_name}` "
               f"\nTotal Records Processed: *{len(df.index)}*")

    send_notification(message, SLACK_CONN)
