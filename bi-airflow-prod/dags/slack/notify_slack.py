from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta
import requests

# Define the Python function to send a Slack message
def send_slack_message():
    url = 'https://slack.com/api/chat.postMessage'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer zghS8DXHELXbPpZljqocWIuX',  # Replace with your token
    }
    payload = {
        'channel': '#bi_loves_notifications',  # Replace with your channel name
        'text': 'Hello, this is a notification from Airflow!',
    }
    response = requests.post(url, headers=headers, json=payload)
    print("response", response)
    if response.status_code != 200:
        raise ValueError(f'Request to Slack API failed with status code {response.status_code}, response: {response.text}')
    elif response.status_code == 200:
        raise ValueError(f'Request to Slack API failed with status code {response.status_code}, response: {response.text}')

# Define default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'slack_notification_dag',
    default_args=default_args,
    description='A DAG to send notifications to Slack',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Define the PythonOperator task
slack_notification = PythonOperator(
    task_id='send_slack_message',
    python_callable=send_slack_message,
    dag=dag,
)

# Set the task sequence (only one task here)
slack_notification
