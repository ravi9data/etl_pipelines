config = [
    {
        'is_full_load': True,
        'source_schema': 'order_approval',
        'source_table': 'customer_labels2',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'customer_labels2',
        'chunk_size': 100000,
        'max_rows_by_file': 100000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '750M',
            'request_cpu': '500m',
            'limit_memory': '2G',
            'limit_cpu': '1000m'
            }
        }
     },
    # this load needs to be ingested before HH:20, where there is a dependency from airflow-ds
    {
        'is_full_load': True,
        'source_schema': 'order_approval',
        'source_table': 'customer_labels_state_changes3',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'customer_labels_state_changes3',
        'chunk_size': 100000,
        'max_rows_by_file': 100000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '750M',
            'request_cpu': '500m',
            'limit_memory': '2G',
            'limit_cpu': '1000m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'order_approval',
        'source_table': 'customer_labels3',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'customer_labels3',
        'chunk_size': 100000,
        'max_rows_by_file': 100000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '750M',
            'request_cpu': '500m',
            'limit_memory': '2G',
            'limit_cpu': '1000m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'order_approval',
        'source_table': 'experian_data',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'experian_data',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '250m',
            'limit_memory': '1G',
            'limit_cpu': '500m'
            }
        }
    }
]
