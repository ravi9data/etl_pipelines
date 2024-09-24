config = [
     {
        'is_full_load': True,
        'source_schema': 'flowing_mosel',
        'source_table': 'auth_user_groups',
        'target_s3_prefix': 'rds_datawarehouse/flowing_mosel/',
        'target_glue_table': 'auth_user_groups',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '500m',
            'limit_memory': '1G',
            'limit_cpu': '1000m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'flowing_mosel',
        'source_table': 'auth_group',
        'target_s3_prefix': 'rds_datawarehouse/flowing_mosel/',
        'target_glue_table': 'auth_group',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '500m',
            'limit_memory': '1G',
            'limit_cpu': '1000m'
            }
        }
     },
]
