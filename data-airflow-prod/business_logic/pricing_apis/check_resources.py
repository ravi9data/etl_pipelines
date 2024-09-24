import datetime
import logging

import pandas as pd
import requests
from airflow.models import DagRun
from airflow.providers.amazon.aws.hooks.redshift import RedshiftSQLHook

from plugins.utils.slack import send_notification
from plugins.utils.sql import read_sql_file

MOZENDA_AGENT_IDS = [1043]
MIN_DAYS_DAG_RUN_RANGE = 3


def list_rainforest_collections(run_date: str, api_key: str, api_domain: str):
    """
    List all collections from Rainforest API.
    """

    page_num = 1
    total_pages = 1

    collection_data = []

    while total_pages >= page_num:
        params = {
            'api_key': api_key,
            'page': page_num
        }
        api_response = requests.get(f'https://{api_domain}/collections',
                                    params=params).json()

        page_num = api_response['current_page'] + 1
        total_pages = api_response['total_pages']

        for number, collection in enumerate(api_response['collections'], start=1):
            if collection['name'] == run_date:
                collection_data.append({
                    "id": collection["id"],
                    "status": collection["status"]
                })

    return collection_data


def get_dag_run_status(dag_id, n_days=MIN_DAYS_DAG_RUN_RANGE):
    """
    Get the status of the most recent dag run in the last n days.
    """

    dag_run = get_most_recent_dag_run(dag_id)
    past = (datetime.datetime.now()
            - datetime.timedelta(days=n_days)).replace(tzinfo=datetime.timezone.utc)
    if dag_run:
        return {"outdated": dag_run.execution_date < past, "execution_date": dag_run.execution_date}
    return {"outdated": True, "execution_date": dag_run.execution_date}


def get_most_recent_dag_run(dag_id):
    """
    Get the most recent dag run by Id.
    """
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    return dag_runs[0] if dag_runs else None


def check_last_run_rainforest_pricing_api():
    """
    Check the last run of the Rainforest Pricing API DAG.
    """

    dag_id = 'rainforest_pricing_api'
    logging.info(f'Checking last run for "${dag_id}".')

    dag_stats = get_dag_run_status(dag_id)

    report_message = 'Rainforest DAG Runner Report:\n'
    last_execution_date = dag_stats['execution_date']

    success_message = f"The \"{dag_id}\" is outdated."
    error_message = f"The \"{dag_id}\" is working as expected."
    report_title = success_message if dag_stats['outdated'] else error_message

    report_message = f"""
    {report_title}
    The last execution date was executed at: {last_execution_date}
    """

    logging.info(report_message)
    send_notification(report_message, slack_connection_id='slack_dataload_notifications')

    return dag_stats['outdated']


def check_mozenda_runner(run_date: str, api_domain: str, api_key: str):
    """
    Check if the Mozenda agents are running on mozenda.com.
    """

    logging.info(f'Checking Mozenda runner for {run_date}')

    params = (
        ("Service", "Mozenda10"),
        ("Operation", "Agent.GetList"),
        ("WebServiceKey", api_key),
        ("ResponseFormat", "JSON")
    )

    try:
        api_response = requests.get(f'https://{api_domain}/rest', params=params).json()

        agents_in_use = []

        if api_response:
            report_agent = ""
            for agent in api_response.get('Agent', []):
                if agent.get('AgentID') in MOZENDA_AGENT_IDS:
                    agents_in_use.append({
                        "Id": agent.get('AgentID', None),
                        "Status": agent.get('Status', None)
                    })

                    status = "running" if agent.get('Status') == 'Running' else " not running"
                    report_agent += f"Agent {agent.get('AgentID')} is {status}.\n"

            report_title = "Mozenda Runner Report:"
            report_agent = report_agent if agents_in_use else "Mozenda isn't running any agents."

            report_message = f"""
            {report_title}
            {report_agent}
            """

            logging.info(report_message)
            send_notification(report_message, slack_connection_id='slack_dataload_notifications')

            status_running = [agent['Status'] == 'Running' for agent in agents_in_use]
            return False if not status_running else all(status_running)
    except Exception as e:
        error_message = f"Failed to check Mozenda runner. Error: {e}"
        logging.info(error_message)
        send_notification(error_message, slack_connection_id='slack_dataload_notifications')

    return False


def check_rainforest_runner(run_date: str, api_domain: str, api_key: str):
    """
    Check if the Rainforest collections are running on rainforest.com.
    """

    logging.info(f'Checking Rainforest runner for {run_date}')

    collection_data = list_rainforest_collections(run_date, api_key, api_domain)
    status_running = [x['status'] == 'running' for x in collection_data]
    has_running_collection = False if not collection_data else all(status_running)

    collection_ids = ', '.join([str(x['id']) for x in collection_data])

    success_message = f"""
    Rainforest is running some collections.
    (collection ids: {collection_ids})
    """
    error_message = """
    Rainforest isn't running any collections.
    (Please check the Rainforest API for more information,
    and also DAG logs for more information about schedule and time gaps.)
    """

    report_message = success_message if has_running_collection is True else error_message

    logging.info(report_message)
    send_notification(report_message)

    return has_running_collection


def check_last_synced_date(run_date: str):
    """
    Check if the Mozenda, and Rainforest synchronization is up to date on RedShift.
    By default, the check is done for the last 3 days.
    """

    past = datetime.date.today() - datetime.timedelta(days=3)
    redshift_engine = RedshiftSQLHook().get_sqlalchemy_engine()

    last_synced_dates_df = pd.read_sql_query(
        con=redshift_engine,
        sql=read_sql_file('./business_logic/pricing_apis/sql/check_resources.sql'),
    )

    last_mozenda_synced_date = last_synced_dates_df['last_mozenda_synced_date'].iloc[0]
    last_rainforest_synced_date = last_synced_dates_df['last_rainforest_synced_date'].iloc[0]

    sync_status = {
        "mozenda": last_mozenda_synced_date >= past,
        "rainforest": last_rainforest_synced_date >= past
    }

    mozenda_report_message = ""
    rainforest_report_message = ""

    if sync_status["mozenda"] is True:
        mozenda_report_message = f"""
        The data synchronization from Mozenda is up to date.
        The last data synchronization from Mozenda occured on {last_mozenda_synced_date}.
        """
    else:
        mozenda_report_message = f"""
        The data synchronization from Mozenda is outdated.
        The last data synchronization was from {last_mozenda_synced_date}.
        """

    if sync_status['rainforest'] is True:
        rainforest_report_message = f"""
        The data synchronization from Rainforest is up to date.
        The last data synchronization from Rainforest occured on {last_rainforest_synced_date}.
        """
    else:
        rainforest_report_message = f"""
        The data synchronization from Rainforest is very outdated.
        The last data synchronization was from {last_rainforest_synced_date}.
        """

    message = f"""
    Data synchronization status report:
    {mozenda_report_message}
    {rainforest_report_message}
    """

    logging.info(message)
    send_notification(message, slack_connection_id='slack_dataload_notifications')

    status_synced = sync_status.values()
    return False if not status_synced else all(status_synced)
