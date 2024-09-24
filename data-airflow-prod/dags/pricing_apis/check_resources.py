import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic.pricing_apis.check_resources import (
    check_last_run_rainforest_pricing_api, check_last_synced_date,
    check_mozenda_runner, check_rainforest_runner)
from plugins.dag_utils import get_schedule

DAG_ID = 'check_resources'
RUN_DATE = (datetime.date.today() - datetime.timedelta(days=0)).strftime('%Y%m%d')

default_args = {
    'owner': 'data-eng',
    'start_date': datetime.datetime(2024, 4, 3),
}

rainforest_config = {
    'api_domain': 'api.rainforestapi.com',
    'api_key_secret_var': 'rainforest_api_key',
}

mozenda_config = {
    'api_domain': 'api.mozenda.com',
    'api_key_secret_var': 'mozenda_api_key',
}

dag = DAG(
    dag_id=DAG_ID,
    doc_md=f"""
        ### {DAG_ID}
        - Check if LUXCO input sources are responding correctly.
        """,
    description='Check if LUXCO input sources are responding correctly.',
    tags=['api', 'prices', 'luxco', 'rainforest', 'mozenda'],
    schedule_interval=get_schedule('0 8 * * *'),
    catchup=False,
    default_args=default_args,
)

trigger_check_last_run_rainforest_pricing_api = PythonOperator(
    task_id='check_last_run_rainforest_pricing_api',
    dag=dag,
    python_callable=check_last_run_rainforest_pricing_api,
    op_kwargs={
        "run_date": RUN_DATE,
    }
)

trigger_check_rainforest_runner = PythonOperator(
    task_id='check_rainforest_runner',
    dag=dag,
    python_callable=check_rainforest_runner,
    op_kwargs={
        "run_date": RUN_DATE,
        "api_domain": rainforest_config["api_domain"],
        "api_key": f'{{{{ var.value.{rainforest_config["api_key_secret_var"]} }}}}',
    }
)

trigger_check_last_synced_date = PythonOperator(
    task_id='check_last_synced_date',
    dag=dag,
    python_callable=check_last_synced_date,
    op_kwargs={
        "run_date": RUN_DATE,
    }
)

trigger_check_mozenda_runner = PythonOperator(
    task_id='check_mozenda_runner',
    dag=dag,
    python_callable=check_mozenda_runner,
    op_kwargs={
        "run_date": RUN_DATE,
        "api_domain": mozenda_config["api_domain"],
        "api_key": f'{{{{ var.value.{mozenda_config["api_key_secret_var"]} }}}}',
    }
)
