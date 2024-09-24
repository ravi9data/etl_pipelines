import logging

import pandas as pd
from airflow.exceptions import AirflowException
from awswrangler.s3 import read_parquet

logger = logging.getLogger(__name__)


def check_unique_records(**kwargs):
    """
    This function helps us to check if we are writing unique orders in RedShift table
    - **manual_review_df**: is a dataframe were we read the result pah of the data we wrote
    in s3 during the prepare data phase
    - **unique_orders**: We calculate the unqur orders in the dataframe

    If the uniquer orders not equal to manual review df length then we are not writing unique orders
    we should raise a slack notification and the DAG must fail

    """
    task_instance = kwargs['ti']
    written_result_path = task_instance.xcom_pull(
        key='result', task_ids='prepare_manual_review_data'
    )
    logger.info(f'Returned value of written_result_path {written_result_path["paths"]}')
    manual_review_df = read_parquet(
        written_result_path['paths'],
        map_types=True,
        ignore_index=True
    )
    unique_orders = len(pd.unique(manual_review_df['order_id']))
    rows_number = len(manual_review_df)
    if unique_orders != rows_number:
        logger.error('*Error: We have duplicates orders in our records*')
        raise AirflowException('*Error: We have duplicates orders in our records*')
