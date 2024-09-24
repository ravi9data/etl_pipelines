import datetime
import logging
import os

from business_logic.customer_labels4.config import (BUCKET_NAME,
                                                    PIPELINE_OUTPUT_PATH,
                                                    TABLE_COLUMNS)
from business_logic.customer_labels4.constants import COUNTRIES_LIST
from business_logic.customer_labels4.data_processing import (create_labels,
                                                             process_features)
from business_logic.customer_labels4.query import query_customers_data
from plugins.utils.s3_utils import get_s3_client


def customer_labels_task(current_time):

    tmp_data_dir = "tmp_data.csv"
    if os.path.exists(tmp_data_dir):
        os.remove(tmp_data_dir)

    logging.info("Run pipeline for each country")
    for country in COUNTRIES_LIST:
        logging.info(f"Processing country: {country}")

        try:
            for rows in query_customers_data(country, "normal_customer", batch_size=10000):

                logging.info("Transform features")
                data = process_features(rows)

                logging.info("Assign labels")
                data = create_labels(data, ruleset_version="us_b2c_v0")

                logging.info("Include timestamp and version")
                data.loc[:,"created_at"] = datetime.datetime.today()
                data.loc[:,"rule_version"] = "us_b2c_v0"

                logging.info("Select table attributes")
                data = data.loc[:, TABLE_COLUMNS]

                logging.info("Write to tmp file")
                data.to_csv(tmp_data_dir, index=False, header=False if os.path.exists(tmp_data_dir) else True, mode="a")
        except Exception as e:
            logging.info(f"EXCEPTION E: {e}")
            break

    s3_path = PIPELINE_OUTPUT_PATH.replace("date_re", current_time)
    logging.info(f"upload to s3: {s3_path}")

    s3_client = get_s3_client()
    s3_client.upload_file("tmp_data.csv", BUCKET_NAME, s3_path)
