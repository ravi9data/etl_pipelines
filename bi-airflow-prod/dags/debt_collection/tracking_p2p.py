import datetime as dt
import logging

import sqlalchemy
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.utils.gsheet import load_data_from_gsheet

DAG_ID = "debt_p2p_collections"
REDSHIFT_CONN = "redshift_default"
GOOGLE_CLOUD_CONN = "google_cloud_default"
SHEET_ID = "1z6_Bhl1z8-HaXw7XbU6zQiTaZw6UwYdvuHaYqlTyhX8"

logger = logging.getLogger(__name__)

default_args = {"owner": "bi-eng", "depends_on_past": False,
                "start_date": dt.datetime(2023, 4, 25),
                "retries": 2,
                "retry_delay": dt.timedelta(seconds=5),
                "retry_exponential_backoff": True,
                "on_failure_callback": on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          description="Extract P2P data from Gsheet and loads into redshift",
          schedule_interval=get_schedule("0 18 * * *"),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, "debt collection", "P2P"])

extract_de_p2p_dc_data = PythonOperator(
    dag=dag,
    task_id="extract_de_p2p_dc_data",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SHEET_ID,
               "range_name": "debt_collection_DE",
               "target_table": "debt_collection_de",
               "target_schema": "staging_airbyte_bi",
               "data_types": {"contact_date": sqlalchemy.types.VARCHAR(length=65535),
                              "contact_type": sqlalchemy.types.VARCHAR(length=65535),
                              "customer_id": sqlalchemy.types.VARCHAR(length=65535),
                              "promised_amount": sqlalchemy.types.VARCHAR(length=65535),
                              "promised_date": sqlalchemy.types.VARCHAR(length=65535),
                              "agent": sqlalchemy.types.VARCHAR(length=65535)
                              }
               },
)

extract_nl_p2p_dc_data = PythonOperator(
    dag=dag,
    task_id="extract_nl_p2p_dc_data",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SHEET_ID,
               "range_name": "debt_collection_NL",
               "target_table": "debt_collection_nl",
               "target_schema": "staging_airbyte_bi",
               "data_types": {"contact_date": sqlalchemy.types.VARCHAR(length=65535),
                              "contact_type": sqlalchemy.types.VARCHAR(length=65535),
                              "customer_id": sqlalchemy.types.VARCHAR(length=65535),
                              "promised_amount": sqlalchemy.types.VARCHAR(length=65535),
                              "promised_date": sqlalchemy.types.VARCHAR(length=65535),
                              "agent": sqlalchemy.types.VARCHAR(length=65535)
                              }
               },
)

extract_es_p2p_dc_data = PythonOperator(
    dag=dag,
    task_id="extract_es_p2p_dc_data",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SHEET_ID,
               "range_name": "debt_collection_ES",
               "target_table": "debt_collection_es",
               "target_schema": "staging_airbyte_bi",
               "data_types": {"contact_date": sqlalchemy.types.VARCHAR(length=65535),
                              "contact_type": sqlalchemy.types.VARCHAR(length=65535),
                              "customer_id": sqlalchemy.types.VARCHAR(length=65535),
                              "promised_amount": sqlalchemy.types.VARCHAR(length=65535),
                              "promised_date": sqlalchemy.types.VARCHAR(length=65535),
                              "agent": sqlalchemy.types.VARCHAR(length=65535)
                              }
               },
)

extract_at_p2p_dc_data = PythonOperator(
    dag=dag,
    task_id="extract_at_p2p_dc_data",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SHEET_ID,
               "range_name": "debt_collection_AT",
               "target_table": "debt_collection_at",
               "target_schema": "staging_airbyte_bi",
               "data_types": {"contact_date": sqlalchemy.types.VARCHAR(length=65535),
                              "contact_type": sqlalchemy.types.VARCHAR(length=65535),
                              "customer_id": sqlalchemy.types.VARCHAR(length=65535),
                              "promised_amount": sqlalchemy.types.VARCHAR(length=65535),
                              "promised_date": sqlalchemy.types.VARCHAR(length=65535),
                              "agent": sqlalchemy.types.VARCHAR(length=65535)
                              }
               },
)

extract_us_p2p_dc_data = PythonOperator(
    dag=dag,
    task_id="extract_us_p2p_dc_data",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SHEET_ID,
               "range_name": "debt_collection_US",
               "target_table": "debt_collection_us",
               "target_schema": "staging_airbyte_bi",
               "data_types": {"contact_date": sqlalchemy.types.VARCHAR(length=65535),
                              "contact_type": sqlalchemy.types.VARCHAR(length=65535),
                              "customer_id": sqlalchemy.types.VARCHAR(length=65535),
                              "promised_amount": sqlalchemy.types.VARCHAR(length=65535),
                              "promised_date": sqlalchemy.types.VARCHAR(length=65535),
                              "agent": sqlalchemy.types.VARCHAR(length=65535)
                              }
               },
)

extract_return_asset_data = PythonOperator(
    dag=dag,
    task_id="extract_return_assets_data",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SHEET_ID,
               "range_name": "return_assets",
               "target_table": "return_assets",
               "target_schema": "staging_airbyte_bi",
               "data_types": {"contact_date": sqlalchemy.types.VARCHAR(length=65535),
                              "contact_type": sqlalchemy.types.VARCHAR(length=65535),
                              "subscription_id": sqlalchemy.types.VARCHAR(length=65535),
                              "country": sqlalchemy.types.VARCHAR(length=65535),
                              "agent": sqlalchemy.types.VARCHAR(length=65535),
                              "dca": sqlalchemy.types.VARCHAR(length=65535)
                              }
               },
)

tracking_p2p = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="tracking_p2p",
    sql="./sql/tracking_p2p.sql"
)

returned_assets = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="returned_assets",
    sql="./sql/returned_assets.sql"
)

extract_de_p2p_dc_data >> extract_nl_p2p_dc_data >> extract_es_p2p_dc_data \
    >> extract_at_p2p_dc_data >> extract_us_p2p_dc_data >> extract_return_asset_data \
    >> tracking_p2p >> returned_assets
