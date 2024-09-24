import logging

from airflow.models import Variable
from awswrangler.athena import read_sql_query

from business_logic.manual_review_us.config.config import list_of_tables

logger = logging.getLogger(__name__)


def get_order_based_query(
 table_name, columns, list_orders, filter_event_names, orders_based_lookback_days):
    columns = ',\n'.join(columns)
    if filter_event_names:
        return f"""
    WITH data_process AS(
        SELECT
            {columns},
            ROW_NUMBER()OVER (
                PARTITION BY order_id ORDER BY consumed_at DESC, updated_at DESC
            ) AS rows
        FROM {table_name}
        WHERE year||month||day >= date_format(
            current_date - interval '{orders_based_lookback_days}' day , '%Y%m%d')
        AND order_id IN {list_orders}
        AND event_name = '{filter_event_names}'
        )
    SELECT
        {columns}
    FROM data_process
    WHERE rows = 1
    """
    else:
        return f"""
    WITH data_process AS(
        SELECT
            {columns},
            ROW_NUMBER()OVER (
                PARTITION BY order_id ORDER BY consumed_at DESC, updated_at DESC
            ) AS rows
        FROM {table_name}
        WHERE year||month||day >= date_format(
            current_date - interval '{orders_based_lookback_days}' day , '%Y%m%d')
        AND order_id IN {list_orders}
        )
    SELECT
        {columns}
    FROM data_process
    WHERE rows = 1
    """


def get_customers_based_query(
 table_name, columns, list_recurring_customers, orders_based_lookback_days):
    columns = ',\n'.join(columns)
    return f"""
    WITH data_process AS(
        SELECT
            {columns},
            ROW_NUMBER()OVER (
                PARTITION BY customer_id ORDER BY consumed_at DESC, updated_at DESC) AS rows
        FROM {table_name}
        WHERE year||month||day>= date_format(
            current_date - interval '{orders_based_lookback_days}' day , '%Y%m%d')
        AND customer_id IN {list_recurring_customers}
        )
    SELECT
        {columns}
    FROM data_process
    WHERE rows = 1
    """


def read_data(
 table_name, columns, list_recurring_customers,
 list_orders, filter_event_names, glue_db_name_write, glue_db_varname, workgroup,
 customers_based_lookback_days, orders_based_lookback_days):
    if list_recurring_customers:
        return read_sql_query(
            sql=get_customers_based_query(
                table_name, columns, list_recurring_customers, customers_based_lookback_days),
            database=glue_db_varname,
            ctas_database_name=glue_db_name_write,
            keep_files=False,
            workgroup=workgroup,
        )
    if list_orders:
        return read_sql_query(
            sql=get_order_based_query(
                table_name, columns, list_orders, filter_event_names, orders_based_lookback_days),
            database=glue_db_varname,
            ctas_database_name=glue_db_name_write,
            keep_files=False,
            workgroup=workgroup
        )


def filter_event_names(input_df, event_name):
    return input_df[input_df['event_name'] == event_name]


def base_data_preparation(
 list_orders, list_recurring_customers, glue_db_name_write, workgroup,
 orders_based_lookback_days, customers_based_lookback_days) -> dict:
    """
    This function is about preparing different data sets:
    1. Loop through the manual_review_operations config
    2. Read the table, deduplication and write it in a df
    3. flatten the payload data to columns
    4. keep only the needed columns
    5. append the dataset to dict {}
    """
    logger.info('Starting the data preparations')
    data = {}
    for data_conf in list_of_tables:
        table_name = data_conf['glue_table_name']
        columns = data_conf['columns_name']
        logger.info(f'Preparing {table_name} with those columns {columns}')
        if data_conf.get('customer_based'):
            deduplicated_data = read_data(
                table_name=table_name,
                list_recurring_customers=list_recurring_customers,
                list_orders=None,
                filter_event_names=None,
                columns=columns,
                glue_db_name_write=glue_db_name_write,
                glue_db_varname=Variable.get(data_conf['glue_db_varname']),
                workgroup=workgroup,
                customers_based_lookback_days=customers_based_lookback_days,
                orders_based_lookback_days=orders_based_lookback_days)
        else:
            deduplicated_data = read_data(
                table_name=table_name,
                list_recurring_customers=None,
                list_orders=list_orders,
                filter_event_names=data_conf.get('event_name_to_filter'),
                columns=columns,
                glue_db_name_write=glue_db_name_write,
                glue_db_varname=Variable.get(data_conf['glue_db_varname']),
                workgroup=workgroup,
                customers_based_lookback_days=customers_based_lookback_days,
                orders_based_lookback_days=orders_based_lookback_days)
        logger.info(f'length of {data_conf["dataframe_name"]} is {len(deduplicated_data)}')
        logger.info(f'Columns are {deduplicated_data.columns}')
        if data_conf.get('column_to_flatten') is not None:
            column_to_flatten = data_conf['column_to_flatten']
            flattened_data = data_conf["python_flattening_func"](
                deduplicated_data, column_to_flatten)
            logger.info(f'length of {data_conf["dataframe_name"]} after flattening is \
                {len(flattened_data)}')
            logger.info(f'Columns after flattening {flattened_data.columns}')
            data[data_conf['dataframe_name']] = flattened_data
        else:
            data[data_conf['dataframe_name']] = deduplicated_data
    return data
