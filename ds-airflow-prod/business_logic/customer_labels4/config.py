DAG_ID="customer_labels4"
BUCKET_NAME="grover-eu-central-1-production-data-science"
TEMP_TABLE_NAME="customer_labels4_temp"
HISTORICAL_TABLE_NAME="customer_labels4_historical"
CURRENT_TABLE_NAME="customer_labels4_current"
TABLE_COLUMNS=["customer_id", "label", "rule", "trust_type", "rule_version", "created_at"]
PIPELINE_OUTPUT_PATH="customer_labels4/date_re/customer_labels4.csv"
REDSHIFT_SCHEMA="data_science"
EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '3G',
        'request_cpu': '2000m',
        'limit_memory': '5G',
        'limit_cpu': '4000m'
    }
}
