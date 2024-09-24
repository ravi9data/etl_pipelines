import logging

from airflow.models import Variable

from plugins.utils.slack import send_notification

logger = logging.getLogger(__name__)


def notifier(**context):
    xcom_task_id = context['params']['xcom_task_id']
    xcom_value = context['ti'].xcom_pull(key='return_value', task_ids=xcom_task_id)
    tasks = xcom_value['tasks']
    logger.info(tasks)

    config_variable = context['params']['config_variable']
    connector_conf = Variable.get(config_variable, deserialize_json=True)
    connector_name = connector_conf['connector']
    states = [t['state'] for t in tasks if t['state'] == 'FAILED']

    if len(states) > 0:
        send_notification(f'Connector {connector_name} reached the state FAILED ')
        raise Exception(f'Connector {connector_name} reached the state FAILED ')
