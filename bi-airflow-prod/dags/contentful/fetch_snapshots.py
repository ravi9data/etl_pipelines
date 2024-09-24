import json
import logging

import airflow.providers.amazon.aws.hooks.redshift_sql as rs
import pandas as pd
import sqlalchemy
from airflow.models import Variable
from contentful_management import Client
from dateutil.parser import parse
from pytz import utc

logger = logging.getLogger(__name__)

# For Product Data - Grover Legacy
LEGACY_SPACE_ID = "6rbx5b6zjte6"

# For Pricing data - Grover Group GmbH
SPACE_ID = "1kxe1uignlmb"
ENV = 'master'

PD_CONTENT_TYPES = ['product', 'experice_widget_2']
PR_CONTENT_TYPES = ['productListWidget', 'handPickedProductWidget', 'landingPage',
                    'RelatedProductsWidget', 'TrendingProductsWidget']


def get_cf_entries(space_id, env, client, page_limit, skip):
    logger.info(f'Fetch contentful entries : skip - {skip}, limit - {page_limit}')
    entries_list = client.entries(space_id, env).all(
        {'limit': page_limit, 'skip': skip, 'order': '-sys.createdAt'})
    return entries_list


def get_entry_snapshots(client, space_id, entry_id, content_type, extract_date):
    columns_list = ['extract_date', 'entry_id', 'content_type', 'snapshot_id', 'snapshot_type',
                    'environment', 'first_published_at', 'published_counter', 'published_version',
                    'published_at', 'published_by', 'default_locale', 'created_at', 'updated_at',
                    'fields'
                    ]

    try:
        logger.info(f'Fetch contentful snapshots for entry id : {entry_id}, type: {content_type}')
        cf_snapshots = client.snapshots(space_id, ENV, entry_id).all({'limit': 1000})
        row_list = []
        for cf_snapshot in cf_snapshots:
            if cf_snapshot.sys['updated_at'] > extract_date:
                row = [extract_date,
                       entry_id,
                       content_type,
                       cf_snapshot.sys['id'],
                       cf_snapshot.sys['snapshot_type'],
                       cf_snapshot.sys['environment'].sys['id'],
                       cf_snapshot.snapshot.sys['first_published_at'],
                       cf_snapshot.snapshot.sys['published_counter'],
                       cf_snapshot.snapshot.sys['published_version'],
                       cf_snapshot.snapshot.sys['published_at'],
                       cf_snapshot.snapshot.sys['published_by'].sys['id'],
                       cf_snapshot.snapshot.default_locale,
                       cf_snapshot.sys['created_at'],
                       cf_snapshot.sys['updated_at'],
                       json.dumps(cf_snapshot.raw['snapshot']['fields'])
                       ]
                row_list.append(row)
        snapshot_df = pd.DataFrame(row_list, columns=columns_list)
        return snapshot_df
    except Exception as ex:
        logging.info(f'Unable to fetch snapshots for entry id : {entry_id}, type: {content_type}')
        logging.info(str(ex))
        return pd.DataFrame(columns=columns_list)


def get_snapshots_for_all_entries(space_id, content_types, extract_date):
    ACCESS_TOKEN = Variable.get('cfm_access_token', default_var='')
    client = Client(ACCESS_TOKEN)
    page_limit = 500
    skip = 0
    df = pd.DataFrame()
    entries_list = get_cf_entries(space_id, ENV, client, page_limit, skip)
    while True:
        for entry in entries_list:
            try:
                content_type = entry.sys['content_type'].sys['id']
                if content_type in content_types \
                        and entry.sys['updated_at'] > extract_date \
                        and entry.sys['published_counter'] > 0:
                    entry_id = entry.sys['id']
                    snapshots = get_entry_snapshots(client, space_id, entry_id, content_type,
                                                    extract_date)
                    df = df.append(snapshots, ignore_index=True)
            except Exception as ex:
                logging.info(f'Unable to process the entry : {entry} - {str(ex)}')

        # next iteration
        if entries_list.total >= skip + page_limit:
            skip += page_limit
            entries_list = get_cf_entries(space_id, ENV, client, page_limit, skip)

        else:
            return df


def cf_snapshot_to_redshift(**kwargs):
    extract_date = parse(kwargs['extract_date']).replace(minute=0, hour=0, second=0, microsecond=0,
                                                         tzinfo=utc)

    logger.info(f'Extract contentful data modified after {extract_date} from space : {SPACE_ID}')
    pricing_snapshots_df = get_snapshots_for_all_entries(SPACE_ID, PR_CONTENT_TYPES, extract_date)

    logger.info(f'Extract contentful data modified after {extract_date} '
                f'from space : {LEGACY_SPACE_ID}')
    product_snapshots_df = get_snapshots_for_all_entries(LEGACY_SPACE_ID, PD_CONTENT_TYPES,
                                                         extract_date)

    df = pd.concat([pricing_snapshots_df, product_snapshots_df], axis=0)

    if len(df.index) > 0:
        conn = rs.RedshiftSQLHook('redshift_default').get_sqlalchemy_engine()
        logger.info('Write batch to database')
        df.to_sql(con=conn, method='multi', chunksize=500, schema='pricing',
                  name='tmp_contentful_snapshots', index=False, if_exists='replace',
                  dtype={'fields': sqlalchemy.types.NVARCHAR(length=65535)})
        # continue with next task
        return True
    else:
        # skip next task
        return False
