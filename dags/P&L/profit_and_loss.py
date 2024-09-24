import datetime as dt
import logging

import sqlalchemy
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.utils.gsheet import load_data_from_gsheet

DAG_ID = "Profit_and_loss_statement"
REDSHIFT_CONN = "redshift_default"
GOOGLE_CLOUD_CONN = "google_cloud_default"
SHEET_ID = "1VH8tsXxW6baHdNHZAledbCbsAULJlvIrO-v9jBOag0c"

logger = logging.getLogger(__name__)

default_args = {"owner": "bi-eng", "depends_on_past": False,
                "start_date": dt.datetime(2023, 4, 25),
                "retries": 2,
                "retry_delay": dt.timedelta(seconds=5),
                "retry_exponential_backoff": True,
                "on_failure_callback": on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          description="Profit and loss statement pipeline",
          schedule_interval=get_schedule("0 06 15 * *"),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, "P_and_L", "profit and loss", "Controlling"])

P_and_L_input_data = PythonOperator(
    dag=dag,
    task_id="P_and_L_input_data",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SHEET_ID,
               "range_name": "p&l_import_sheet",
               "target_table": "p_l_import_sheet",
               "target_schema": "staging_airbyte_bi",
               "data_types": {"dimension": sqlalchemy.types.VARCHAR(length=65535),
                              "date": sqlalchemy.types.VARCHAR(length=65535),
                              "cost_entity": sqlalchemy.types.VARCHAR(length=65535),
                              "customer_type": sqlalchemy.types.VARCHAR(length=65535),
                              "amount": sqlalchemy.types.VARCHAR(length=65535)
                              }
               },
)

redshift = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="01_redshift",
    sql="./sql/01_redshift.sql"
)

input_script = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="02_input_script",
    sql="./sql/02_input_script.sql"
)

revenue_sources = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="03_revenue_sources",
    sql="./sql/03_revenue_sources.sql"
)

variable_cost = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="04_variable_cost",
    sql="./sql/04_variable_cost.sql"
)

marketing_costs = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="05_marketing_costs",
    sql="./sql/05_marketing_costs.sql"
)

other_expenses = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="06_other_expenses",
    sql="./sql/06_other_expenses.sql"
)

final = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="07_final",
    sql="./sql/07_final.sql"
)

Unpivot_final = PostgresOperator(
    dag=dag,
    postgres_conn_id='redshift_default',
    task_id="08_unpivot_final",
    sql="./sql/08_unpivot_final.sql"
)


P_and_L_input_data >> redshift >> input_script >> revenue_sources \
   >> variable_cost >> marketing_costs >> other_expenses \
   >> final >> Unpivot_final
