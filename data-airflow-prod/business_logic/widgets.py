import json
import logging

import boto3
import pandas as pd
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.redshift import RedshiftSQLHook

logger = logging.getLogger(__name__)

CONN_ID = 'redshift'


def agg_list(df, grouped_by, agg_element, column_name):

    logger.info(f'Transforming dataframe into a new dataframe grouped by \
                {grouped_by} and aggregated by {agg_element}')
    df_agg = df.groupby(by=grouped_by)[agg_element].apply(list).reset_index()
    df_agg[column_name] = df_agg[agg_element].apply(lambda x: {column_name: x})
    df_agg.reset_index()

    logger.info(f'Returning dataframe grouped by {grouped_by} and aggregated by {agg_element}')
    logger.info('Showing first 5 rows of the aggregated Dataframe')
    logger.info(df_agg[:5])

    return df_agg


def get_alchemy_connection(conn_id):
    src_postgres_con = RedshiftSQLHook(conn_id).get_sqlalchemy_engine()
    logger.info('Returning connection')
    return src_postgres_con


def widget_data(
        table_name,
        schema,
        prefix,
        grouped_by,
        agg_element,
        column_name,
        sort_column,
        sort_ascending=True):

    s3 = boto3.client('s3')
    bucket = Variable.get('widget_bucket')
    engine = get_alchemy_connection(CONN_ID)

    df_raw = pd.read_sql_table(
        table_name=table_name,
        con=engine,
        schema=schema,
    )

    logger.info(f'Table {schema}.{table_name} loaded into DataFrame with len {len(df_raw)}')

    if df_raw.empty:
        raise NameError(f'{schema}.{table_name} is empty. Please, check with BI.'
                        f'Data was not deleted from S3.')

    sort_by = grouped_by + sort_column
    df_ordered = df_raw.sort_values(by=sort_by, ascending=sort_ascending)

    df_agg = agg_list(
        df=df_ordered,
        grouped_by=grouped_by,
        agg_element=agg_element,
        column_name=column_name)

    for i in df_agg.index:
        df_to_write = df_agg[i:i+1].reset_index()
        object_to_write = df_to_write[column_name][0]

        concatenated_prefix = ''
        for element in grouped_by:
            concatenated_prefix += str(df_to_write[element][0]) + '/'

        s3_key = f'{prefix}{concatenated_prefix}data.json'

        logger.info(f'Writing Dataframe column {column_name}:{object_to_write} '
                    f'to s3://{bucket}/{s3_key}')

        s3.put_object(
            Body=json.dumps(object_to_write),
            Bucket=bucket,
            Key=s3_key)
