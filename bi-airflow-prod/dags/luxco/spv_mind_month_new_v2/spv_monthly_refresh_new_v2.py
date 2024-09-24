import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

from business_logic.luxco.spv_mid_month.send_email_notification import \
    send_email_notification
from plugins.dag_utils import on_failure_callback
from plugins.slack_utils import send_notification

REDSHIFT_CONN = 'redshift_default'

first_day_of_month = datetime.today().replace(day=1)
mid_day_of_month = datetime.today() - timedelta(days=1)  # .replace(day=15)
last_day_of_prev_month = first_day_of_month - timedelta(days=1)
date_for_depreciation = last_day_of_prev_month.replace(day=1)
tbl_suffix = date_for_depreciation.strftime('%Y%m') + '_mid_month'

DAG_ID = 'spv_mid_month_new_v2'

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime(2022, 10, 20),
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


def prep_manual_revisions():
    mr_sql = f"""
        select 'update ods_production.spv_report_master_{tbl_suffix}
            set final_price = ' || final_price || '
            where product_sku = '''|| product_sku ||'''
            and capital_source_name = ''' || capital_source || '''
            and final_price <> 0
            and asset_condition_spv = ''AGAN''' as manual_revision_query
        from ods_spv_historical.luxco_manual_revisions;
    """

    rs_hook = get_db_hook()
    results = rs_hook.get_records(mr_sql)

    mr_update_sql = '; '.join(str(rec[0]) for rec in results)

    logging.info(mr_update_sql)
    # rs_hook.run(sql=mr_update_sql, autocommit=True, split_statements=True)


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='DAG to Run Luxco pipeline during the mid of the month',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID, 'luxco'],
)

load_manual_price_collection = AirbyteTriggerSyncOperator(
    task_id='airbyte_load_manual_price_collection',
    airbyte_conn_id='airbyte_prod',
    connection_id='02e219ad-b8a5-435c-a438-52f8bca22ec6',
    asynchronous=False,
    timeout=600,
    wait_seconds=60,
    dag=dag
)

prep_mr = PythonOperator(
    task_id="prep_manual_revision",
    python_callable=prep_manual_revisions,
    dag=dag
)

prep_md = PythonOperator(
    task_id="prep_manual_deletions",
    python_callable=prep_manual_deletions,
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
            "mid_day_of_month": mid_day_of_month.strftime('%Y-%m-%d'),
            "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
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
                    "mid_day_of_month": mid_day_of_month.strftime('%Y-%m-%d'),
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
                    "mid_day_of_month": mid_day_of_month.strftime('%Y-%m-%d'),
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

spv_us = SQLExecuteQueryOperator(
    dag=dag,
    task_id='spv_us',
    conn_id=REDSHIFT_CONN,
    sql='./sql/spv_us.sql',
    autocommit=True,
    split_statements=True,
    params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
            "mid_day_of_month": mid_day_of_month.strftime('%Y-%m-%d'),
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
            "mid_day_of_month": mid_day_of_month.strftime('%Y-%m-%d'),
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
            "mid_day_of_month": mid_day_of_month.strftime('%Y-%m-%d'),
            "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
            "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
            "tbl_suffix": tbl_suffix}
)

insert_deletes = SQLExecuteQueryOperator(
    dag=dag,
    task_id='insert_deletes',
    conn_id=REDSHIFT_CONN,
    sql='./sql/insert_deletes.sql',
    autocommit=True,
    split_statements=True,
    params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
            "mid_day_of_month": mid_day_of_month.strftime('%Y-%m-%d'),
            "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
            "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
            "tbl_suffix": tbl_suffix}
)

luxco_reporting = SQLExecuteQueryOperator(
    dag=dag,
    task_id='luxco_reporting',
    conn_id=REDSHIFT_CONN,
    sql='./sql/luxco_reporting.sql',
    autocommit=True,
    split_statements=True,
    params={"first_day_of_month": first_day_of_month.strftime('%Y-%m-%d'),
            "mid_day_of_month": mid_day_of_month.strftime('%Y-%m-%d'),
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
            "mid_day_of_month": mid_day_of_month.strftime('%Y-%m-%d'),
            "last_day_of_prev_month": last_day_of_prev_month.strftime('%Y-%m-%d'),
            "date_for_depreciation": date_for_depreciation.strftime('%Y-%m-%d'),
            "tbl_suffix": tbl_suffix}
)

send_status_notification = PythonOperator(
    task_id='send_status_notification',
    dag=dag,
    python_callable=send_notification,
    op_kwargs={'message': f"*Luxco Mid Month Pipeline:* \nExecution completed for reporting "
                          f"month: *{first_day_of_month.strftime('%Y-%m-%d')}*"}
)

send_email = PythonOperator(
    task_id='send_email',
    dag=dag,
    python_callable=send_email_notification,
    op_kwargs={"email_recipient": ["erkin@grover.com", "nasim.pezeshki@grover.com",
                                   "saad.amir@grover.com", "giulio@grover.com",
                                   "gabrielgrunberg@grover.com", "fabio.zaim@grover.com",
                                   "francisco@grover.com", "data-platform@grover.com"],
               "email_subject": "Luxco Mid Month Metrics"}
)

load_manual_price_collection >> union_sources >> prep_mr >> prep_md >> spv_us \
    >> insert_deletes >> luxco_reporting >> luxco_reporting_snapshot \
    >> send_status_notification >> send_email

load_manual_price_collection >> union_sources >> prep_mr >> prep_md \
    >> integrated_step_group >> ranking >> step_5_and_6 >> insert_deletes \
    >> luxco_reporting >> luxco_reporting_snapshot \
    >> send_status_notification >> send_email
