from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from plugins.slack_utils_bi import send_notification

DAG_ID = 'data_quality'
REDSHIFT_CONN = 'redshift_default'
SLACK_CONN = "slack_bi"


def check_low_paid_submitted_per_country_and_recurring_type():
    hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN)
    conn = hook.get_conn()

    yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    query = f"""
    SELECT store_country, new_recurring, ratio_paid,
    ratio_submitted, is_low_paid_number, is_low_submitted_number
    FROM staging.country_and_new_recurring_order_alerts
    WHERE (is_low_paid_number = true OR is_low_submitted_number = true)
    AND submitted_date = '{yesterday_str}';
    """

    try:
        send_notification("final_message", "slack_bi")
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

            if results:
                alert_messages = []
                for row in results:
                    store_country = row[0]
                    recurring_type = row[1]
                    ratio_paid = row[2]
                    ratio_submitted = row[3]
                    is_low_paid = row[4]
                    is_low_submitted = row[5]

                    # Send a message only for the true flag
                    if is_low_paid:
                        alert_messages.append(
                            f"Alert: {store_country} ({recurring_type}) - "
                            f"Low Paid Orders Ratio: {ratio_paid} on {yesterday_str}."
                        )

                    if is_low_submitted:
                        alert_messages.append(
                            f"Alert: {store_country} ({recurring_type}) - "
                            f"Low Submitted Orders Ratio: {ratio_submitted} on {yesterday_str}."
                        )

                if alert_messages:
                    final_message = "\n".join(alert_messages)
                    send_notification(final_message, "slack_bi")

    finally:
        conn.close()


def check_low_paid_submitted_global():
    hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN)
    conn = hook.get_conn()

    yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    query = f"""
    SELECT submitted_date, ratio_paid, ratio_submitted, is_low_paid_number, is_low_submitted_number
    FROM staging.global_order_alerts
    WHERE submitted_date = '{yesterday_str}';
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()

            if result:
                submitted_date = result[0]
                ratio_paid = result[1]
                ratio_submitted = result[2]
                is_low_paid = result[3]
                is_low_submitted = result[4]

                alert_messages = []

                if is_low_paid:
                    alert_messages.append(
                        f"Alert: Low Paid Orders Ratio: {ratio_paid} on {submitted_date}."
                    )

                if is_low_submitted:
                    alert_messages.append(
                        f"Alert: Low Submitted Orders Ratio: {ratio_submitted} on {submitted_date}."
                    )

                if alert_messages:
                    final_message = "\n".join(alert_messages)
                    send_notification(final_message)

    finally:
        conn.close()


default_args = {
    'owner': 'bi-eng',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 20),
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Daily check for low paid and submitted alerts',
    schedule_interval='0 7 * * *',  # Cron expression for daily at 7 AM
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID, 'order_comparison', 'data_quality']
)

country_and_new_recurring_order_alerts = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id='country_and_new_recurring_order_alerts',
    sql="./sql/country_and_new_recurring_order_alerts.sql"
)

global_order_alerts = PostgresOperator(
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    task_id='global_order_alerts',
    sql="./sql/global_order_alerts.sql"
)

check_alerts_1 = PythonOperator(
    task_id='check_low_paid_submitted_per_country_and_recurring_type',
    python_callable=check_low_paid_submitted_per_country_and_recurring_type,
    dag=dag
)

check_alerts_2 = PythonOperator(
    task_id='check_low_paid_submitted_global',
    python_callable=check_low_paid_submitted_global,
    dag=dag
)

country_and_new_recurring_order_alerts >> check_alerts_1
global_order_alerts >> check_alerts_2
