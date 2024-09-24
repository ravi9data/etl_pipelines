import datetime
import logging

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import pandas as pd
from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

from dags.supplier_reporting.extract_data import extract_supplier_report_weekly
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'supplier_report_automation'

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bi',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 6, 1),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15),
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=30)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Send Supplier reports every Monday',
    schedule_interval=get_schedule('15 9 * * MON'),
    max_active_runs=1,
    catchup=False,
    tags=['supplier', 'reporting']
)


def send_weekly_supplier_reports():
    today = pendulum.now()
    start_of_week = today.start_of('week')

    try:
        rs = rd.RedshiftSQLHook(redshift_conn_id='redshift_default')
        conn = rs.get_conn()
        logging.info('Extracting reports configurations from Database config table.')
        df = pd.read_sql_query('select id,brand_name,email_list,supplier_name,cc_email_list '
                               ',sql_query,category_name,country, gdrive_folder_id '
                               'from dm_brand_reporting.supplier_reporting_config '
                               'where is_active =1 ', conn)
        # conn.close()

        for index, row in df.iterrows():
            brand_name = row['brand_name']
            sql_query = row['sql_query']
            gdrive_folder_id = row['gdrive_folder_id']

            filename = 'Weekly_Report_{0}.xlsx'.format(start_of_week.strftime("%Y_%m_%d"))

            logging.info('Extracting Data for : {}'.format(brand_name))
            extract_supplier_report_weekly(conn, filename, sql_query, gdrive_folder_id)

    except Exception as e:
        logging.error('Error Occurred : {}'.format(e))
        raise AirflowException


send_reports = PythonOperator(
    dag=dag,
    task_id='send_reports',
    python_callable=send_weekly_supplier_reports
)
