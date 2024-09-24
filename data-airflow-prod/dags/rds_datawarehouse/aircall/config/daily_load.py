config = [
     {
        'is_full_load': True,
        'source_schema': 'aircall',
        'source_table': 'aircall_call',
        'target_s3_prefix': 'rds_datawarehouse/aircall/',
        'target_glue_table': 'aircall_call',
        'chunk_size': 25000,
        'max_rows_by_file': 25000,
        'executor_config': {'KubernetesExecutor': {
            'limit_memory': '1G',
            'limit_cpu': '500m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'aircall',
        'source_table': 'aircall_user',
        'target_s3_prefix': 'rds_datawarehouse/aircall/',
        'target_glue_table': 'aircall_user',
        'executor_config': {'KubernetesExecutor': {
            'limit_memory': '1G',
            'limit_cpu': '500m'
        }
        }
     }
]
