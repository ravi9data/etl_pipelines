import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from business_logic import geolocation as geolocation_loader
from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = 'geolocation_all_countries'

logger = logging.getLogger(f'{DAG_ID}')

CONFIG = [
    {
        'task_name': 'gazeteer_data',
        'url': 'http://download.geonames.org/export/dump/allCountries.zip',
        'glue_table_name': 'geolocation_gazeteer',
        's3_bucket_prefix': 'geolocation/gazeteer',
        'column_names': [
            'geoname_id',
            'geographical_name_utf8',
            'geographical_name_ascii',
            'geographical_alternate_names',
            'latitude',
            'longitude',
            'feature_class',
            'feature_code',
            'country_code',
            'country_codes_csv',
            'postal_code',
            'state_code',
            'province_code',
            'community_code',
            'population',
            'elevation',
            'dem',
            'timezone',
            'modification_date'
        ],
        'chunk_size': 500000,
        'execution_config': {
            'KubernetesExecutor':
            {
                'request_memory': '12G',
                'request_cpu': '1500m',
                'limit_memory': '14G',
                'limit_cpu': '4000m'
            }
        }
    },
    {
        'task_name': 'all_postal_code_data',
        'url': 'http://download.geonames.org/export/zip/allCountries.zip',
        'glue_table_name': 'geolocation_postal_code',
        's3_bucket_prefix': 'geolocation/postal_code',
        'column_names': [
            'country_code',
            'postal_code',
            'place_name',
            'state_name',
            'state_code',
            'province_name',
            'province_code',
            'community_name',
            'community_code',
            'latitude',
            'longitude',
            'accuracy'
        ],
        'chunk_size': 1000000,
        'execution_config': {
            'KubernetesExecutor':
            {
                'request_memory': '1G',
                'request_cpu': '250m',
                'limit_memory': '2G',
                'limit_cpu': '1000m'
            }
        }
    }]


default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 12, 7),
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback,
    'execution_timeout': datetime.timedelta(minutes=15)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Load geolocation data `gazeteer` and `all_postal_code` to the data lake',
    schedule_interval=get_schedule('45 3,13 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=['geolocation', 'external', 'data-lake']
)

for conf in CONFIG:
    gelocation_load_data = PythonOperator(
        dag=dag,
        task_id=f'load_{conf.get("task_name")}',
        python_callable=geolocation_loader.geolocation_writer,
        params={
            'url': conf.get('url'),
            'glue_table_name': conf.get('glue_table_name'),
            's3_bucket_prefix': conf.get('s3_bucket_prefix'),
            'column_names': conf.get('column_names'),
            'chunk_size': conf.get('chunk_size')
        },
        executor_config=conf.get('execution_config')
    )
