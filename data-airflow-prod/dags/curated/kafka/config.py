import datetime

from business_logic.curated.kafka.internal_billing_payments import \
    transformation
from plugins.dag_utils import on_failure_callback

# from business_logic.curated.kafka.operations_order_allocated import \
#     add_new_columns

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "start_date": datetime.datetime(2021, 12, 17),
    "retries": 3,
    "retry_delay": datetime.timedelta(seconds=20),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure_callback,
    "execution_timeout": datetime.timedelta(minutes=30)
}

curated_operations = [
    # {
    #     "dag_id": "curated_kafka_transformations_operations_order_allocated",
    #     "dag_schedule": "*/30 * * * *",
    #     "s3_source_bucket":  Variable.get('curated_s3_source_bucket'),
    #     "s3_source_prefix": "kafka_topics/operations_order_allocated",
    #     "glue_database": "data_production_kafka_topics_curated",
    #     "glue_table_name": Variable.get('curated_glue_database'),
    #     "s3_destination_bucket": Variable.get('curated_s3_destination_bucket'),
    #     "s3_destination_prefix": "kafka_topics/operations_order_allocated",
    #     "python_transformation_func": add_new_columns,
    #     "config_variable_name": "curated_dag_id",
    #     "columns_from_source": ['event_name', 'payload', 'consumed_at']
    # },
    {
        "dag_id": "curated_kafka_internal_billing_payments",
        "dag_schedule": "*/30 * * * *",
        "s3_source_prefix": "kafka_topics/internal_billing_payments",
        "glue_table_name": "internal_billing_payments",
        "s3_destination_prefix": "kafka_topics/internal_billing_payments",
        "python_transformation_func": transformation,
        "config_variable_name": "curated_kafka_internal_billing_payments",
        "executor_config": {
            "KubernetesExecutor": {
                "request_memory": "1Gi",
                "request_cpu": "500m",
                "limit_memory": "2Gi",
                "limit_cpu": "1000m"
            }
        }
    }
]
