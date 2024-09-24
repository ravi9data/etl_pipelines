from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.rainforest_countdown.send_email_notification import \
    send_email_notification
from plugins.dag_utils import on_failure_callback

DAG_ID = 'rainforest_countdown_weekly_metrics'

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime(2024, 8, 2),
                'retries': 2,
                'on_failure_callback': on_failure_callback}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='DAG to Send Email Notification of Rainforest and Countdown Metrics weekly',
    schedule_interval='30 9 * * 5',
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID, 'Rainforest-Countdown'],
)

send_email = PythonOperator(
    task_id='send_email',
    dag=dag,
    python_callable=send_email_notification,
    op_kwargs={"email_recipient": ["erkin@grover.com",
                                   "nasim.pezeshki@grover.com",
                                   "saad.amir@grover.com",
                                   "data-platform@grover.com"],
               "email_subject": "Rainforest Countdown Weekly Metrics"}
)
