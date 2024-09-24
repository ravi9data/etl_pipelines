import io
import logging
import re
import uuid
from datetime import datetime

import awswrangler as wr
import pandas as pd
import pysftp


def ingram_ogs_sftp_to_s3(run_date: str):
    target_columns = ['order_number',
                      'serial_number',
                      'item_number',
                      'shipping_number',
                      'disposition_code',
                      'status_code',
                      'source_timestamp',
                      'partner_id',
                      'asset_serial_number',
                      'package_serial_number',
                      'questions',
                      'shipment_type',
                      'date_of_processing',
                      'carrier_out',
                      'carrier_service',
                      'track_and_trace',
                      'customer',
                      'country',
                      'package_no',
                      'serial_no',
                      'assetname',
                      'damage',
                      'hc_code',
                      'item_category',
                      'reporting_date',
                      'source_id']

    sftp_host = 'ftp.ims-flensburg.com'
    sftp_user = 'grover'
    sftp_password = 'hMu38.X3957'
    run_date = run_date.replace('-', '')
    sftp_path = f"transfer/I2G/sendGradingOrderStatusProcessed/{run_date}"

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    try:
        # connect to sftp server
        with pysftp.Connection(host=sftp_host, username=sftp_user, password=sftp_password,
                               cnopts=cnopts) as sftp:
            # get a list of all files in the remote directory
            logging.info(f'Processing file in {sftp_path}')
            remote_files = sftp.listdir(sftp_path)
            if remote_files:
                remote_files.sort()

                # create an empty DataFrame to store the data
                data = pd.DataFrame(columns=target_columns)

                # loop through each file and append its data to the DataFrame
                for file in remote_files:
                    # read the file from the remote directory into a buffer
                    remote_file = io.BytesIO()
                    sftp.getfo(sftp_path + '/' + file, remote_file)
                    remote_file.seek(0)

                    # parse the JSON data
                    json_data = pd.read_json(remote_file, lines=True)

                    # add the filename to the DataFrame as a new column
                    json_data['source_id'] = file

                    # extract the reporting date from the filename and add it as a new column
                    reporting_date_str = re.search(r'\d{4}-\d{2}-\d{2}', file).group()
                    json_data['reporting_date'] = datetime.strptime(reporting_date_str, '%Y-%m-%d').date()  # noqa: E501

                    # append the data to the DataFrame
                    data = pd.concat([data, json_data], ignore_index=True)

                # generate a batch_id for the data
                batch_id = str(uuid.uuid4())

                # add the batch_id and extracted_at columns to the DataFrame
                data['batch_id'] = batch_id
                data['extracted_at'] = datetime.now()

                s3_bucket = 's3://grover-eu-central-1-production-data-bi-curated/staging/ingram'
                s3_destination_path = f'{s3_bucket}/ogs_{run_date}.parquet'

                for col in data.columns:
                    data[col] = data[col].astype('string')

                wr.s3.to_parquet(df=data, path=s3_destination_path)

            else:
                logging.info('No files available to process')
    except Exception as ex:
        logging.info(str(ex))
