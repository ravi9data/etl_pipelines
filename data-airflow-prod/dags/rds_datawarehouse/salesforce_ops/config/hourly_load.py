config = [
     {
        'is_full_load': False,
        'source_timestamp_field': '"updatedAt"',
        'source_schema': 'salesforce_ops',
        'source_table': 'asset_delivery',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/salesforce_ops/',
        'target_glue_table': 'asset_delivery',
        'lowercase_columns': True,
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
