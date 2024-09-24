import datetime
import logging
import random

from airflow.models import Variable
from dateutil.relativedelta import relativedelta

from plugins.utils.trino.client import TrinoClient
from plugins.utils.trino.config import get_trino_variables

logger = logging.getLogger(__name__)


def get_glue_redshift_db():
    return Variable.get('glue_database_redshift_datasets')


def query_ml_user_session():
    host, user, password = get_trino_variables()
    client = TrinoClient(user=user, password=password, host=host)
    glue_database = get_glue_redshift_db()
    user_sample_size = 100

    random_users = client.query_dict(f"""WITH dataset as (
    SELECT
        user_id,
        ROW_NUMBER() over (partition by user_id ORDER BY RANDOM()) AS seq_num
    FROM glue.{glue_database}.ml_userid_session_id_matching
    )
    SELECT DISTINCT user_id
    FROM dataset
    WHERE seq_num <= 3
    LIMIT {user_sample_size}
    """)
    logger.info(random_users)
    users = [i['user_id'] for i in random_users]
    logger.info(users)

    def _get_query(user_id):
        return f"""SELECT user_id, user_id2, creation_time
        FROM glue.{glue_database}.ml_userid_session_id_matching
        WHERE user_id = {user_id}
        ORDER BY creation_time
    """

    for _ in range(500):
        query = _get_query(users[random.randint(0, len(users)-1)])
        data = client.query_df(query)
        logger.info(f'Extracted dataset: {len(data)} rows')


def query_ml_user_session_bucketed():
    host, user, password = get_trino_variables()
    client = TrinoClient(user=user, password=password, host=host)
    glue_database = get_glue_redshift_db()
    user_sample_size = 100

    random_users = client.query_dict(f"""WITH dataset as (
    SELECT
        user_id,
        ROW_NUMBER() over (partition by user_id ORDER BY RANDOM()) AS seq_num
    FROM glue.{glue_database}.ml_userid_session_id_matching_bucketed
    )
    SELECT DISTINCT user_id
    FROM dataset
    WHERE seq_num <= 3
    LIMIT {user_sample_size}
    """)
    logger.info(random_users)
    users = [i['user_id'] for i in random_users]
    logger.info(users)

    def _get_query(user_id):
        return f"""SELECT user_id, user_id2, creation_time
        FROM glue.{glue_database}.ml_userid_session_id_matching_bucketed
        WHERE user_id = {user_id}
        ORDER BY creation_time
    """

    for _ in range(500):
        query = _get_query(users[random.randint(0, len(users)-1)])
        data = client.query_df(query)
        logger.info(f'Extracted dataset: {len(data)} rows')


def query_geolocation():
    host, user, password = get_trino_variables()
    client = TrinoClient(user=user, password=password, host=host)
    glue_database = Variable.get('glue_database_geolocation')
    query = f"""SELECT country_code, count(*) AS c
    FROM glue.{glue_database}.geolocation_postal_code
    WHERE concat(cast(year as varchar), month, day) =(
        SELECT MAX(CONCAT(cast(year as varchar), month, day))
        FROM glue.{glue_database}.geolocation_postal_code
        )
    GROUP BY 1
    ORDER BY 1
    """
    data = client.query_df(query)
    logger.info(f'Extracted dataset: {len(data)}')
    logger.info(data)


def get_last_month():
    now = datetime.datetime.utcnow() - relativedelta(months=1)
    year = str(now.year)
    month = "{:02d}".format(now.month)
    return year, month


def query_last_month_snowplow():
    host, user, password = get_trino_variables()
    client = TrinoClient(user=user, password=password, host=host)
    glue_database = Variable.get('glue_db_kafka')
    year, month = get_last_month()

    query = f"""
    SELECT
        CAST(PARSE_DATETIME(year || month || day, 'YYYYMMdd') AS date) AS d,
        COUNT(1) AS events,
        COUNT(DISTINCT event_id) AS distinct_events,
        COUNT(DISTINCT user_id) AS users,
        COUNT(distinct "$path") AS s3_objects
    FROM glue.{glue_database}.streaming_snowplow_event_parser_parsed
    WHERE
    year='{year}' AND month='{month}'
    GROUP by 1
    ORDER by 1
    """
    data = client.query_df(query)
    logger.info(f'Extracted dataset: {len(data)}')
    logger.info(data)


def runner(func):
    """
    Simple wrapper function to call queries function
    """
    func()
