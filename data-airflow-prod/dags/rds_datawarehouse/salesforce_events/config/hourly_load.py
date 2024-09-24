config = [
    {
        'is_full_load': True,
        'source_schema': 'salesforce_events',
        'source_table': 'asset',
        'target_s3_prefix': 'rds_datawarehouse/salesforce_events/',
        'target_glue_table': 'asset',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '500m',
            'limit_memory': '4G',
            'limit_cpu': '2000m'
            }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': 'date',
        'source_schema': 'salesforce_events',
        'source_table': 'asset_payment',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/salesforce_events/',
        'target_glue_table': 'asset_payment',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '2000m'
            }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': 'date',
        'source_schema': 'salesforce_events',
        'source_table': 'subscription_payment',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/salesforce_events/',
        'target_glue_table': 'subscription_payment',
        'chunk_size': 50000,
        'max_rows_by_file': 50000,
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '1000m',
            'limit_memory': '12G',
            'limit_cpu': '10000m'
            }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': 'date',
        'source_schema': 'salesforce_events',
        'source_table': 'allocation',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/salesforce_events/',
        'target_glue_table': 'allocation',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '500m',
            'limit_memory': '4G',
            'limit_cpu': '2000m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'salesforce_events',
        'source_table': 'payment',
        'target_s3_prefix': 'rds_datawarehouse/salesforce_events/',
        'target_glue_table': 'payment',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '500m',
            'limit_memory': '4G',
            'limit_cpu': '2000m'
            }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': 'date',
        'source_schema': 'salesforce_events',
        'source_table': 'refurbishment',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/salesforce_events/',
        'target_glue_table': 'refurbishment',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '500m',
            'limit_memory': '4G',
            'limit_cpu': '2000m'
            }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': 'date',
        'source_schema': 'salesforce_events',
        'source_table': 'subscription',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/salesforce_events/',
        'target_glue_table': 'subscription',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '500m',
            'limit_memory': '4G',
            'limit_cpu': '2000m'
            }
        }
    },
]
