config = [
    {
        'is_full_load': False,
        'source_timestamp_field': '"LastModifiedDate"',
        'source_schema': 'salesforce_skyvia',
        'source_table': '"OrderItem"',
        'source_uid_column': 'Id',
        'lowercase_columns': True,
        'target_s3_prefix': 'rds_datawarehouse/salesforce_skyvia/',
        'target_glue_table': 'orderitem',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '500m',
            'limit_memory': '6G',
            'limit_cpu': '4000m'
        }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': '"LastModifiedDate"',
        'source_schema': 'salesforce_skyvia',
        'source_table': '"Order"',
        'source_uid_column': 'Id',
        'lowercase_columns': True,
        'target_s3_prefix': 'rds_datawarehouse/salesforce_skyvia/',
        'target_glue_table': 'orders',
        'chunk_size': 7500,
        'max_rows_by_file': 7500,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '500m',
            'limit_memory': '8G',
            'limit_cpu': '4000m'
        }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': '"LastModifiedDate"',
        'source_schema': 'salesforce_skyvia',
        'source_table': 'subscription__c',
        'source_uid_column': 'Id',
        'lowercase_columns': True,
        'target_s3_prefix': 'rds_datawarehouse/salesforce_skyvia/',
        'target_glue_table': 'subscription__c',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '500m',
            'limit_memory': '8G',
            'limit_cpu': '4000m'
            }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': '"LastModifiedDate"',
        'source_schema': 'salesforce_skyvia',
        'source_table': '"Account"',
        'source_uid_column': 'Id',
        'lowercase_columns': True,
        'target_s3_prefix': 'rds_datawarehouse/salesforce_skyvia/',
        'target_glue_table': 'account',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '500m',
            'limit_memory': '6G',
            'limit_cpu': '4000m'
        }
        }
    }
]
