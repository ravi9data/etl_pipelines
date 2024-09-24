import logging

from airflow.models import Variable
from func_timeout import FunctionTimedOut, func_set_timeout

from plugins.utils.trino.client import TrinoClient
from plugins.utils.trino.config import get_trino_variables

logger = logging.getLogger(__name__)


@func_set_timeout(60)
def query_ml_userid_session_id_matching_bucketed():
    host, user, password = get_trino_variables()
    client = TrinoClient(user=user, password=password, host=host)
    glue_database = Variable.get('glue_database_redshift_datasets')

    query = f"""select *
    from glue.{glue_database}.ml_userid_session_id_matching_bucketed
    limit 10
    """
    data = client.query_df(query)
    logger.info(f'Extracted dataset: {len(data)}')
    logger.info(data)


@func_set_timeout(120)
def query_geolocation():
    host, user, password = get_trino_variables()
    client = TrinoClient(user=user, password=password, host=host)
    glue_database = Variable.get('glue_database_geolocation')

    query = f"""select count(distinct country_code) as distinct_country_codes
    from glue.{glue_database}.geolocation_postal_code
    WHERE concat(cast(year as varchar), month, day) =(
        SELECT MAX(CONCAT(cast(year as varchar), month, day))
        FROM glue.{glue_database}.geolocation_postal_code
    )
    """
    data = client.query_df(query)
    logger.info(f'Extracted dataset: {len(data)}')
    logger.info(data)


def runner(func):
    """
    Simple wrapper function to call queries function
    """
    try:
        func()
    except FunctionTimedOut:
        error_message = f'Timeout reached by {func.__name__} function'
        logger.error(error_message)
        # TODO here we can add send a metric to Datadog
        raise Exception(error_message)
