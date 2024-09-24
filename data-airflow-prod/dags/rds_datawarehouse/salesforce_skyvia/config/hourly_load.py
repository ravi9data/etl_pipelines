config = [
    {
        'is_full_load': False,
        'source_timestamp_field': '"LastModifiedDate"',
        'source_schema': 'salesforce_skyvia',
        'source_table': '"Asset"',
        'source_uid_column': 'Id',
        'lowercase_columns': True,
        'target_s3_prefix': 'rds_datawarehouse/salesforce_skyvia/',
        'target_glue_table': 'asset',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '4G',
            'request_cpu': '2500m',
            'limit_memory': '12G',
            'limit_cpu': '4000m'
        }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': '"LastModifiedDate"',
        'source_schema': 'salesforce_skyvia',
        'source_table': '"customer_asset_allocation__c"',
        'source_uid_column': 'Id',
        'lowercase_columns': True,
        'target_s3_prefix': 'rds_datawarehouse/salesforce_skyvia/',
        'target_glue_table': 'customer_asset_allocation__c',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '4G',
            'request_cpu': '2000m',
            'limit_memory': '6G',
            'limit_cpu': '4000m'
        }
        }
    }
]
