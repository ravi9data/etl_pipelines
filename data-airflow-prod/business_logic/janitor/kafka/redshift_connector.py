import logging
import os

import requests
from airflow.models import Variable

from plugins.utils.slack import send_notification

logger = logging.getLogger(__name__)


def get_task_url(connect_url, redshift_connector, task_id, verb):
    return os.path.join(connect_url, 'api/connect/ecs/connectors',
                        redshift_connector, 'tasks', str(task_id), verb)


def process_state(state):
    if state == 'FAILED':
        return 0
    return 1


def continue_restart(tasks):
    states = [process_state(t['state']) for t in tasks]
    return not all(states)


def continue_execution(**context):
    xcom_task_id = context['params']['xcom_task_id']
    xcom_value = context['ti'].xcom_pull(key='return_value', task_ids=xcom_task_id)
    continue_task_id = context['params']['continue_task_id']
    skip_task_id = context['params']['skip_task_id']

    tasks = xcom_value['tasks']
    if continue_restart(tasks):
        return continue_task_id
    return skip_task_id


def restart_connector(**context):
    xcom_task_id = context['params']['xcom_task_id']
    config_variable = context['params']['config_variable']
    connector_status = context['ti'].xcom_pull(key='return_value', task_ids=xcom_task_id)
    logger.debug(connector_status)

    redshift_connect_janitor = Variable.get(config_variable, deserialize_json=True)
    connect_url = redshift_connect_janitor['connect_url']
    redshift_connector = redshift_connect_janitor['connector']
    connector_tasks = connector_status['tasks']

    for task in connector_tasks:
        _id = task['id']
        task_status_url = get_task_url(connect_url, redshift_connector, _id, 'status')
        response_status = requests.get(task_status_url)
        logger.info(response_status.json())
        response_status.raise_for_status()
        if response_status.json()['state'] != 'RUNNING':
            task_restart_url = get_task_url(connect_url, redshift_connector, _id, 'restart')
            response_restart = requests.post(task_restart_url)
            response_restart.raise_for_status()
            logger.info(response_restart.content)
            logger.info(f'Restarted task {_id}')

    logger.info(f'{len(connector_tasks)} successfully restarted!')
    send_notification(f'{redshift_connector} was restarted because was not running!')
