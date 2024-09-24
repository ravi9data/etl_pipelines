import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import sqlalchemy
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.utils.task_group import TaskGroup
from googleapiclient.http import MediaFileUpload

from plugins.dag_utils import on_failure_callback
from plugins.slack_utils import send_notification
from plugins.utils.gsheet import load_data_from_gsheet

REDSHIFT_CONN = 'redshift_default'

first_day_of_month = datetime.today().replace(day=1)
last_day_of_prev_month = first_day_of_month - timedelta(days=1)
date_for_depreciation = last_day_of_prev_month.replace(day=1)
tbl_suffix = date_for_depreciation.strftime('%Y%m')

DAG_ID = 'spv_monthly_refresh_v3'
GOOGLE_CLOUD_CONN = "google_cloud_default"
SHEET_ID = "1KM9sjUxooeqMQZDIrS_5vM4VETL8cewyEZ66afK3cJM"

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime(2023, 9, 1, 8, 0),
                'retries': 2,
                'on_failure_callback': on_failure_callback}


def get_db_hook():
    return RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN)


def prep_manual_deletions():
    md_sql = f"""
        select 'delete from ods_spv_historical.union_sources_{tbl_suffix}_eu
                where region = '''|| upper(region) || '''
                and '|| filter_criteria || ';' AS deletion_query
        from
        (
        select
            luxco_month,region ,
            listagg(' (product_sku ='''||product_sku ||'''
                and ( '|| filter_criteria || '))',' OR ') as filter_criteria
        from ods_spv_historical.luxco_manual_deletions
        WHERE upper(region) = 'GERMANY'
        group by region,luxco_month
        )
        UNION all
        select 'delete from ods_spv_historical.union_sources_{tbl_suffix}_us
                where region = '''|| upper(region) || '''
                and '|| filter_criteria || ';' AS deletion_query
        from
        (
        select
            luxco_month,region ,
            listagg(' (product_sku ='''||product_sku ||'''
                and ( '|| filter_criteria || '))',' OR ') as filter_criteria
        from ods_spv_historical.luxco_manual_deletions
        WHERE upper(region) = 'USA'
        group by region,luxco_month
        )
    """
    rs_hook = get_db_hook()
    results = rs_hook.get_records(md_sql)

    md_cleanup_sql = '; '.join(str(rec[0]) for rec in results)
    md_cleanup_sql = md_cleanup_sql.replace(';;', ';')

    logging.info(md_cleanup_sql)
    rs_hook.run(sql=md_cleanup_sql, autocommit=True, split_statements=True)


def prep_manual_deletions_n():
    md_sql = f"""
        select 'delete from ods_spv_historical.union_sources_{tbl_suffix}_eu_all
                where region = '''|| upper(region) || '''
                and '|| filter_criteria || ';' AS deletion_query
        from
        (
        select
            luxco_month,region ,
            listagg(' (product_sku ='''||product_sku ||'''
                and ( '|| filter_criteria || '))',' OR ') as filter_criteria
        from ods_spv_historical.new_luxco_manual_deletions
        WHERE upper(region) = 'GERMANY'
        group by region,luxco_month
        )
    """

    rs_hook = get_db_hook()
    results = rs_hook.get_records(md_sql)

    md_cleanup_sql = '; '.join(str(rec[0]) for rec in results)
    md_cleanup_sql = md_cleanup_sql.replace(';;', ';')

    logging.info(md_cleanup_sql)
    rs_hook.run(sql=md_cleanup_sql, autocommit=True, split_statements=True)


def export_luxco_report():
    base_path = os.path.dirname(__file__)
    file_name = f'luxco_report_{tbl_suffix}_{datetime.today().strftime("%Y%m%d%H%M%S")}.csv.gz'
    local_path = os.path.join(base_path, file_name)
    drive_folder_id = '1vzhYduzNrpEQqBYVXI1ww5pCuQpWT9vQ'   # Luxco_Extract folder ID

    query = open(os.path.join(base_path, 'sql/luxco_report_extract.sql'), 'r')
    sa_engine = RedshiftSQLHook('redshift_default').get_sqlalchemy_engine()

    logging.info('Extracting report data')
    df = pd.read_sql(sql=query.read(), con=sa_engine)
    df.to_csv(file_name, index=False, compression="gzip")

    gdrive_hook = GoogleDriveHook(gcp_conn_id="google_cloud_default")
    try:
        logging.info('Exporting report data')
        service = gdrive_hook.get_conn()
        file_metadata = {"name": file_name, "parents": [drive_folder_id]}
        media = MediaFileUpload(file_name)
        file = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id", supportsAllDrives=True)
            .execute(num_retries=2)
        )
        logging.info(f"File {file_name} uploaded to gdrive.")
        return file.get("id")

    except FileNotFoundError:
        logging.error(f"File {local_path} can't be found")


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='DAG to Run Integrated Luxco pipeline',
    schedule_interval='30 9 1 * *',
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID, 'luxco'],
)


load_manual_price_collection = PythonOperator(
    dag=dag,
    task_id="load_manual_price_collection",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SHEET_ID,
               "range_name": "luxco_manual_revisions",
               "target_table": "luxco_manual_revisions",
               "target_schema": "ods_spv_historical",
               "data_types": {"luxco_month": sqlalchemy.types.VARCHAR(length=65535),
                              "capital_source": sqlalchemy.types.VARCHAR(length=65535),
                              "asset_condition": sqlalchemy.types.VARCHAR(length=65535),
                              "product_sku": sqlalchemy.types.VARCHAR(length=65535),
                              "final_price": sqlalchemy.types.VARCHAR(length=65535)
                              }
               },
)

load_md_n = PythonOperator(
    dag=dag,
    task_id="load_md_n",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SHEET_ID,
               "range_name": "new_luxco_manual_deletions",
               "target_table": "new_luxco_manual_deletions",
               "target_schema": "ods_spv_historical",
               "data_types": {"luxco_month": sqlalchemy.types.VARCHAR(length=65535),
                              "region": sqlalchemy.types.VARCHAR(length=65535),
                              "product_sku": sqlalchemy.types.VARCHAR(length=65535),
                              "filter_criteria": sqlalchemy.types.VARCHAR(length=65535)
                              }
               },
)


load_md = PythonOperator(
    dag=dag,
    task_id="load_md",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SHEET_ID,
               "range_name": "luxco_manual_deletions",
               "target_table": "luxco_manual_deletions",
               "target_schema": "ods_spv_historical",
               "data_types": {"luxco_month": sqlalchemy.types.VARCHAR(length=65535),
                              "region": sqlalchemy.types.VARCHAR(length=65535),
                              "product_sku": sqlalchemy.types.VARCHAR(length=65535),
                              "filter_criteria": sqlalchemy.types.VARCHAR(length=65535)
                              }
               },
)


prep_md = PythonOperator(
    task_id="prep_manual_deletions",
    python_callable=prep_manual_deletions,
    dag=dag
)


prep_md_n = PythonOperator(
    task_id="prep_manual_deletions_n",
    python_callable=prep_manual_deletions_n,
    dag=dag
)


union_sources = SQLExecuteQueryOperator(
    dag=dag,
    task_id='union_sources',
    conn_id=REDSHIFT_CONN,
    sql='./sql/union_sources.sql',
    autocommit=True,
    split_statements=True,
    params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),

            "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
            "tbl_suffix": tbl_suffix}
)

# discuss implementation of this script
with dag:
    with TaskGroup(group_id='integrated_steps') as integrated_step_group:
        condition_1 = SQLExecuteQueryOperator(
            dag=dag,
            task_id='condition_1',
            conn_id=REDSHIFT_CONN,
            sql='./sql/condition_1.sql',
            autocommit=True,
            split_statements=True,
            params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
                    "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
                    "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
                    "tbl_suffix": tbl_suffix}
        )

        condition_2 = SQLExecuteQueryOperator(
            dag=dag,
            task_id='condition_2',
            conn_id=REDSHIFT_CONN,
            sql='./sql/condition_2.sql',
            autocommit=True,
            split_statements=True,
            params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
                    "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
                    "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
                    "tbl_suffix": tbl_suffix}
        )

        condition_4 = SQLExecuteQueryOperator(
            dag=dag,
            task_id='condition_4',
            conn_id=REDSHIFT_CONN,
            sql='./sql/condition_4.sql',
            autocommit=True,
            split_statements=True,
            params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
                    "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
                    "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
                    "tbl_suffix": tbl_suffix}
        )

        condition_5 = SQLExecuteQueryOperator(
            dag=dag,
            task_id='condition_5',
            conn_id=REDSHIFT_CONN,
            sql='./sql/condition_5.sql',
            autocommit=True,
            split_statements=True,
            params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
                    "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
                    "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
                    "tbl_suffix": tbl_suffix}
        )

        condition_6 = SQLExecuteQueryOperator(
            dag=dag,
            task_id='condition_6',
            conn_id=REDSHIFT_CONN,
            sql='./sql/condition_6.sql',
            autocommit=True,
            split_statements=True,
            params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
                    "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
                    "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
                    "tbl_suffix": tbl_suffix}
        )

        condition_7 = SQLExecuteQueryOperator(
            dag=dag,
            task_id='condition_7',
            conn_id=REDSHIFT_CONN,
            sql='./sql/condition_7.sql',
            autocommit=True,
            split_statements=True,
            params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
                    "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
                    "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
                    "tbl_suffix": tbl_suffix}
        )

        condition_8 = SQLExecuteQueryOperator(
            dag=dag,
            task_id='condition_8',
            conn_id=REDSHIFT_CONN,
            sql='./sql/condition_8.sql',
            autocommit=True,
            split_statements=True,
            params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
                    "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
                    "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
                    "tbl_suffix": tbl_suffix}
        )

spv_step_1_us = SQLExecuteQueryOperator(
    dag=dag,
    task_id='spv_step_1_us',
    conn_id=REDSHIFT_CONN,
    sql='./sql/spv_Step1_US.sql',
    autocommit=True,
    split_statements=True,
    params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
            "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
            "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
            "tbl_suffix": tbl_suffix}
)

ranking = SQLExecuteQueryOperator(
    dag=dag,
    task_id='ranking',
    conn_id=REDSHIFT_CONN,
    sql='./sql/ranking.sql',
    autocommit=True,
    split_statements=True,
    params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
            "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
            "tbl_suffix": tbl_suffix}
)

step_5_and_6 = SQLExecuteQueryOperator(
    dag=dag,
    task_id='step_5_and_6',
    conn_id=REDSHIFT_CONN,
    sql='./sql/step5_and_6.sql',
    autocommit=True,
    split_statements=True,
    params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
            "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
            "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
            "tbl_suffix": tbl_suffix}
)

insert_delete = SQLExecuteQueryOperator(
    dag=dag,
    task_id='insert_delete',
    conn_id=REDSHIFT_CONN,
    sql='./sql/insert_delete.sql',
    autocommit=True,
    split_statements=True,
    params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
            "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
            "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
            "tbl_suffix": tbl_suffix}
)

spv_historical = SQLExecuteQueryOperator(
    dag=dag,
    task_id='spv_historical',
    conn_id=REDSHIFT_CONN,
    sql='./sql/spv_Historical.sql',
    autocommit=True,
    split_statements=True,
    params={"last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d')}
    )

luxco_reporting = SQLExecuteQueryOperator(
    dag=dag,
    task_id='luxco_reporting',
    conn_id=REDSHIFT_CONN,
    sql='./sql/luxco_reporting.sql',
    autocommit=True,
    split_statements=True,
    params={
        "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
        "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
        "tbl_suffix": tbl_suffix}
)


luxco_reporting_snapshot = SQLExecuteQueryOperator(
    dag=dag,
    task_id='luxco_reporting_snapshot',
    conn_id=REDSHIFT_CONN,
    sql='./sql/luxco_reporting_snapshot.sql',
    autocommit=True,
    split_statements=True,
    params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
            "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
            "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
            "tbl_suffix": tbl_suffix}
)

send_status_notification = PythonOperator(
    task_id='send_status_notification',
    dag=dag,
    python_callable=send_notification,
    op_kwargs={'message': f"*Luxco Pipeline:* \nExecution completed for reporting "
                          f"month: *{first_day_of_month.strftime('%Y-%m-%d')}*"}
)

export_luxco_report = PythonOperator(
    task_id='export_luxco_report_to_gdrive',
    dag=dag,
    python_callable=export_luxco_report,
    executor_config={
        'KubernetesExecutor': {
            'request_memory': '800Mi',
            'request_cpu': '500m',
            'limit_memory': '6G',
            'limit_cpu': '1500m'
        }
    }
)


load_manual_price_collection >> union_sources >> [load_md, load_md_n] \
    >> prep_md >> prep_md_n >> integrated_step_group >> ranking >> step_5_and_6 \
    >> insert_delete >> spv_historical >> luxco_reporting >> luxco_reporting_snapshot \
    >> send_status_notification >> export_luxco_report

load_manual_price_collection >> union_sources >> [load_md, load_md_n] \
    >> prep_md >> prep_md_n >> spv_step_1_us \
    >> insert_delete >> spv_historical >> luxco_reporting >> luxco_reporting_snapshot \
    >> send_status_notification >> export_luxco_report
