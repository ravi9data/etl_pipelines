import datetime
import logging
from functools import partial, reduce

import numpy as np
import pandas as pd
from airflow.models import Variable
from awswrangler.athena import read_sql_query
from awswrangler.catalog import extract_athena_types

from business_logic.manual_review_us.config.config import list_of_tables
from business_logic.manual_review_us.data_preparation.base_data_preparation import \
    base_data_preparation
from business_logic.manual_review_us.transformation.transformer import \
    transformer_data
from plugins.s3_utils import get_s3_full_path
from plugins.utils.business_logic_utils import generate_batch_id
from plugins.utils.data_lake_helper import rewrite_data
from plugins.utils.memory import clean_from_memory
from plugins.utils.pandas_helpers import (add_partition_columns,
                                          dicts_columns_to_string,
                                          empty_cols_to_string_type,
                                          sanitize_column_types)
from plugins.utils.redshift import get_alchemy_connection
from plugins.utils.sql import read_sql_file

logger = logging.getLogger(__name__)

REDSHIFT_CONN_ID = 'redshift'


def handle_ids_redshift(ids: tuple):
    """
    Handle edge case of ids with 1 record as tuple as they will be rendered as
    WHERE ids IN ('a',) in the final query.
    This function prevent it, and can let you run queries like WHERE ids IN ('a')
    removing the trailing comma in case of tuple with 1 record
    :param ids: tuple containing customers ids
    """
    if len(ids) == 1:
        return f"('{ids[0]}')"
    return ids


def prepare_list_orders(orders_based_lookback_days):
    """
    This function, read data from redshift table `ods_data_sensitive.order_manual_review`
    get the manual_review orders and write them in a dataframe, then return them as a list
    return: list, list of orders
    """
    logger.info('Read manual review US orders from RedShift')
    sql_query = read_sql_file(
        './business_logic/manual_review_us/sql/redshift_order_inputs.sql').format(
            lookback_days=orders_based_lookback_days)
    mr_orders = pd.read_sql(
        sql=sql_query,
        con=get_alchemy_connection(REDSHIFT_CONN_ID)
    )
    logger.info(f'Number of records retrieved from Redshift: {mr_orders.shape[0]}')
    return tuple(mr_orders['order_id'].to_list())


def prepare_list_recurring_customers(list_orders):
    """
    This function, read data from redshift table `ods_data_sensitive.order_manual_review`
    get the manual_review recurring customers and write them in a dataframe,
    then return them as a list
    return: list, list of recurring customers
    """
    logger.info('Read manual review US recurring customers from RedShift')
    sql_query = read_sql_file(
        './business_logic/manual_review_us/sql/redshift_recurring_customers.sql').format(
            list_orders=list_orders
        )
    mr_recurring_customers = pd.read_sql(
        sql=sql_query,
        con=get_alchemy_connection(REDSHIFT_CONN_ID)
    )
    logger.info(f'Number of records retrieved from Redshift: {mr_recurring_customers.shape[0]}')
    return tuple(mr_recurring_customers['customer_id'].to_list())


def prepare_list_customer_df():
    return tuple(
        [
            table_conf["dataframe_name"] for table_conf in list_of_tables
            if table_conf.get('customer_based') is True
        ]
        )


def join_orders_based_data(data) -> pd.DataFrame:
    """
    In this function we loop through the dict data and we need to merge
    the dataframes that we need to merge on `order_id`
    """
    joined_orders = pd.merge(
        data['flex_order'],
        data['flex_log'],
        how='inner',
        on='order_id',
        indicator="indicator_column",
        suffixes=("_fo", "_fl")
    )
    logger.info(f'joined orders columns {joined_orders.columns}')
    list_customer_based_df = prepare_list_customer_df()
    logger.info(f'The list to filter out is {list_customer_based_df}')
    list_dfs = [joined_orders]
    for df_name, df_val in data.items():
        if df_name not in list_customer_based_df and df_name not in ('flex_order', 'flex_log'):
            list_dfs.append(df_val)
    logger.info(f'This is my list to use for merge {len(list_dfs)}')
    merge = partial(pd.merge, on='order_id', how='left')
    df_merged = reduce(merge, list_dfs)
    logger.info(f'merged dfs columns {df_merged.columns}')
    clean_from_memory(joined_orders)
    return df_merged


def join_customer_based_data(input_df, data) -> pd.DataFrame:
    list_customer_based_df = prepare_list_customer_df()
    logger.info(f'the length of the data before customer based data merge {len(input_df)}')
    logger.info('Rename the "email_address" column to "email"')
    data['seon_email'].rename(columns={
        'email_address': 'email'
    }, inplace=True)
    merged_seon_email = pd.merge(
        input_df,
        data['seon_email'],
        how='left',
        on=['customer_id', 'email']
    )
    merged_seon_email = merged_seon_email[[
        c for c in merged_seon_email.columns if not c.endswith('_y')]]
    merged_seon_email = merged_seon_email.replace(np.nan, '', regex=True)
    logger.info(f'the length of the data after seon email join {len(merged_seon_email)}')
    logger.info(f'output_data df columns {merged_seon_email.columns}')
    list_customer_based_df = prepare_list_customer_df()
    list_dfs = [merged_seon_email]
    for df_name, df_val in data.items():
        if df_name in list_customer_based_df and df_name != 'seon_email':
            list_dfs.append(df_val)
    logger.info(f'This is my list to use for merge {len(list_dfs)}')
    merge = partial(pd.merge, on='customer_id', how='left', suffixes=('', '_delme'))
    df_merged = reduce(merge, list_dfs)
    logger.info(f'merged dfs columns {df_merged.columns}')
    df_merged = df_merged[[c for c in df_merged.columns if not c.endswith('_delme')]]
    logger.info(f'merged dfs columns after renaming {df_merged.columns}')
    return df_merged


def merged_all_data(prepared_data,
                    order_address_stats_df,
                    order_stats_df,
                    order_payment_method_df):
    """
    This function is the preparation part of all the data we need to apply transformation on
    """
    logger.info(f'the length of the data is {len(prepared_data)}')
    merged_order_base_data = join_orders_based_data(prepared_data)
    merged_customer_base_data = join_customer_based_data(merged_order_base_data, prepared_data)
    clean_from_memory(merged_order_base_data)
    merged_order_stats = pd.merge(
        merged_customer_base_data,
        order_stats_df,
        how='left',
        on='customer_id'
    )
    clean_from_memory(merged_customer_base_data)
    merged_order_address_stats = pd.merge(
        merged_order_stats,
        order_address_stats_df,
        how='left',
        on='order_id'
    )
    clean_from_memory(merged_order_stats)
    final_df = pd.merge(
        merged_order_address_stats,
        order_payment_method_df,
        how='left',
        on='customer_id'
    )
    clean_from_memory(merged_order_address_stats)
    clean_from_memory(order_address_stats_df)
    clean_from_memory(order_stats_df)
    clean_from_memory(order_payment_method_df)
    logger.info(f'final df columns {final_df.columns}')
    return final_df


def merge_with_onfido(
  input_df, list_orders, glue_db_varname, glue_db_name_write, workgroup, lookback_days):
    """
    This function execute the SQL wuery in `/business_logic/manual_review_us/sql/onfido_data`
    by parsing the `list_orders` as params
    Join the onfido_df with transformed data `input_data` on `order_id`
    deduplicate data using order_id and return the final dataframe
    """
    onfido_df = read_sql_query(
        sql=read_sql_file('./business_logic/manual_review_us/sql/onfido_data.sql'),
        database=glue_db_varname,
        ctas_database_name=glue_db_name_write,
        keep_files=False,
        workgroup=workgroup,
        params={
            "list_orders": f"{list_orders}",
            "lookback_days": f"{lookback_days}"}
    )
    logger.info(f'Columns of onfido df are {onfido_df.columns}')
    logger.info(f'Number of records got for onfido {len(onfido_df)}')
    joined_data = pd.merge(
        input_df,
        onfido_df,
        how='left',
        on='order_id',
        indicator='exists',
        suffixes=('', '_onfido')
    )
    logger.info(f'Columns of onfido df are {joined_data.columns}')
    del joined_data['exists']
    logger.info(f'Columns joined data before renaming dates {joined_data.columns}')
    logger.info(f'Number of records after joining data {len(joined_data)}')
    return joined_data


def add_subscription_details_data(input_df, list_customers):
    """
    This function is for enriching data coming from customer subsciption details
    1. Read data from redshift table
    2. join the resulted dataframe with the input data
    3. return the data frame with the new information
    """
    logger.info('Read customer subscription details data from RedShift')
    sql_query = read_sql_file(
            './business_logic/manual_review_us/sql/read_customer_subscription_details.sql').format(
                customer_ids=list_customers
            )
    customer_subscription_df = pd.read_sql(
        sql=sql_query,
        con=get_alchemy_connection(REDSHIFT_CONN_ID)
    )
    logger.info(f'Number of records retrieved from Redshift: {customer_subscription_df.shape[0]}')
    output_df = pd.merge(
        input_df,
        customer_subscription_df,
        how='left',
        on='customer_id',
        indicator='exists',
        suffixes=('', '_subscription')
    )
    logger.info(f'Columns of full dataframe are {output_df.columns}')
    del output_df['exists']
    logger.info(f'Number of records after joining data {len(output_df)}')
    return output_df


def add_latest_ffp_order(input_df, list_customers):
    """
    This function is for enriching data coming from customer order details
    1. Read data from redshift table
    2. join the resulted dataframe with the input data
    3. return the data frame with the new information
    """
    logger.info('Read customer subscription details data from RedShift')
    sql_query = read_sql_file(
            './business_logic/manual_review_us/sql/read_order_details.sql').format(
                customer_ids=list_customers
            )
    ffp_orders = pd.read_sql(
        sql=sql_query,
        con=get_alchemy_connection(REDSHIFT_CONN_ID)
    )
    logger.info(f'Number of records retrieved from Redshift: {ffp_orders.shape[0]}')
    output_df = pd.merge(
        input_df,
        ffp_orders,
        how='left',
        on='customer_id',
        indicator='exists'
    )
    logger.info(f'Columns of full dataframe are {output_df.columns}')
    del output_df['exists']
    logger.info(f'Number of records after joining data {len(output_df)}')
    return output_df


def writer_s3(input_df, glue_db_name_write, s3_destination_bucket, s3_bucket_prefix):
    """
    This function doing the following:
    1. Generate a uuid
    2. Add the uuid to the current dataframe, return a new dataframe
    3. Add the written_at field and use it for parition
    4. Sanitize the columns before writing in s3
    5. define the s3 path
    6. write data to s3 with paritions
    """
    partition_columns = ['year', 'month', 'day', 'hour']
    s3_destination_path = get_s3_full_path(s3_destination_bucket, s3_bucket_prefix)
    logger.info(f'Data will be written in {s3_destination_path}')
    batch_id = generate_batch_id()
    logger.info(f'Generated batch_id for this run: {batch_id}')
    output_df = input_df.copy()
    logger.info('Adding batch_id key to input dataframe')
    output_df['batch_id'] = batch_id
    logger.info(f'output df after adding batch_id {output_df.columns}')
    logger.info(f'output columns type {output_df.dtypes}')
    df_sanitized = empty_cols_to_string_type(output_df, all_columns_as_string=True)
    columns_types, partitions_types = extract_athena_types(
        df=df_sanitized,
        index=False,
        file_format='parquet')
    df_sanitized = dicts_columns_to_string(
        df=df_sanitized,
        athena_cols=columns_types)
    sanitize_col_types = sanitize_column_types(columns_types)
    logger.info('sanitize_col_types:')
    logger.info(sanitize_col_types)
    logger.info('Adding inserted time to input dataframe')
    df_sanitized['written_at'] = datetime.datetime.utcnow()
    df_sanitized = add_partition_columns(df_sanitized, 'written_at', partition_columns)
    logger.info(f'columns we are writing to s3 are {df_sanitized.columns}')
    result_writer = rewrite_data(
        s3_destination_path,
        'manual_review_us',
        glue_db_name_write,
        df_sanitized)
    return result_writer, batch_id


def prepare_data(**kwargs):
    """
    This function, presents the core logic of the operational part needed:
    1. Read the returned value from `get_latest_order` task, to have the value of order_id
    2. Create the config variables needed from the configuration variable in airflow
    3. Prepare the list of orders parsing the `order_id` recieved from the spredsheet
    4. Create the dataframe for `order_shipping_address_stat`
    5. Create the dataframe for `order_stats`
    6. Create the datafrane for `order_payment_method`
    7. Loop through the manual_review_config to create the dataset needed for transformation
    8. Transform the merged_data to add other columns
    9. Merge the transformed data with onfido data
    10.Read subscription details data and merge it with the result dataframe
    11. Read order details data to get latest ffp_order
    12. write data to s3 calling writter function
    """
    config = Variable.get('manual_review_config', deserialize_json=True)
    workgroup = Variable.get(
        'sensitive_athena_workgroup',
        default_var='deng-applications-sensitive')
    glue_db_varname = Variable.get('glue_db_kafka')
    glue_db_name_write = config['glue_db_name_write']
    orders_based_lookback_days = config['orders_based_lookback_days']
    customers_based_lookback_days = config['customers_based_lookback_days']
    task_instance = kwargs['ti']
    list_orders = prepare_list_orders(orders_based_lookback_days)
    logger.info(f'We will process {len(list_orders)} orders')
    list_recurring_customers = prepare_list_recurring_customers(list_orders)
    logger.info(f'We will process {len(list_recurring_customers)} recurring customers')
    logger.info(f'Get order shipping stat data of the last {orders_based_lookback_days} days')
    order_address_stats_df = read_sql_query(
        sql=read_sql_file(
            './business_logic/manual_review_us/sql/order_shipping_address_stat.sql'),
        database=glue_db_varname,
        ctas_database_name=glue_db_name_write,
        keep_files=False,
        workgroup=workgroup,
        params={
            "lookback_days": f"{orders_based_lookback_days}"}
    )
    logger.info(f'length of order shipping address stat is {len(order_address_stats_df)}')
    logger.info(f'Get order stat data {orders_based_lookback_days} days')
    order_stats_df = read_sql_query(
        sql=read_sql_file('./business_logic/manual_review_us/sql/order_stats.sql'),
        database=glue_db_varname,
        ctas_database_name=glue_db_name_write,
        keep_files=False,
        workgroup=workgroup,
        params={
            "lookback_days": f"{orders_based_lookback_days}"}
    )
    logger.info(f'length of order stats is {len(order_stats_df)}')
    logger.info(f'Get order payment method data {orders_based_lookback_days} days')
    order_payment_method_df = read_sql_query(
        sql=read_sql_file('./business_logic/manual_review_us/sql/order_payment_method.sql'),
        database=glue_db_varname,
        ctas_database_name=glue_db_name_write,
        keep_files=False,
        workgroup=workgroup,
        params={
            "lookback_days": f"{customers_based_lookback_days}"
        }
    )
    logger.info(f'length of order payment methods is {len(order_payment_method_df)}')
    prepared_data = base_data_preparation(
        list_orders,
        list_recurring_customers,
        glue_db_name_write,
        workgroup,
        orders_based_lookback_days,
        customers_based_lookback_days)
    merged_data = merged_all_data(
        prepared_data=prepared_data,
        order_address_stats_df=order_address_stats_df,
        order_stats_df=order_stats_df,
        order_payment_method_df=order_payment_method_df
    )
    logger.info(f'Number of records merged {len(merged_data)}')
    transformed_df = transformer_data(merged_data)
    clean_from_memory(merged_data)
    logger.info(f'Number of records after transformation {len(transformed_df)}')
    logger.info(f'transformed data columns {transformed_df.columns}')
    data_with_onfido = merge_with_onfido(
        transformed_df,
        list_orders,
        glue_db_varname,
        glue_db_name_write,
        workgroup,
        orders_based_lookback_days)
    clean_from_memory(transformed_df)
    customer_ids = tuple(data_with_onfido['customer_id'].to_list())
    list_customers = handle_ids_redshift(customer_ids)
    # Add step to merge with subscription
    subscription_details = add_subscription_details_data(data_with_onfido, list_customers)
    logger.info(f'Number of records after adding subscription details {len(subscription_details)}')
    clean_from_memory(data_with_onfido)
    logger.info(f'subscription_details columns {subscription_details.columns}')
    final_data = add_latest_ffp_order(subscription_details, list_customers)
    if len(final_data) > 0:
        result, batch_id = writer_s3(
            final_data,
            glue_db_name_write=glue_db_name_write,
            s3_destination_bucket=config['s3_bucket'],
            s3_bucket_prefix=config['s3_bucket_prefix'])
        logger.info(f'Dataset written: {result}, batch id {batch_id}')
        partition = list(result['partitions_values'].values())[0]
        task_instance.xcom_push(key='year', value=partition[0])
        task_instance.xcom_push(key='month', value=partition[1])
        task_instance.xcom_push(key='day', value=partition[2])
        task_instance.xcom_push(key='hour', value=partition[3])
        task_instance.xcom_push(key='batch_id', value=batch_id)
        task_instance.xcom_push(key='result', value=result)
        task_instance.xcom_push(key='written_records', value=len(final_data))
        task_instance.xcom_push(key='orders_number', value=len(list_orders))
    else:
        logger.info('DataFrame is empty there is no data to write')
