config = [
     {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'assets',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'assets',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '250m',
            'limit_memory': '1G',
            'limit_cpu': '500m'
            }
        }
     },
     {
        'is_full_load': True,  # small table with ~400 records
        'source_schema': 'api_production',
        'source_table': 'brands',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'brands',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '250m',
            'limit_memory': '500M',
            'limit_cpu': '500m'
            }
        }
     },
     {
        'is_full_load': True,  # small table with ~100 records
        'source_schema': 'api_production',
        'source_table': 'categories',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'categories',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '250m',
            'limit_memory': '500M',
            'limit_cpu': '500m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'eans',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'eans',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '250m',
            'limit_memory': '1G',
            'limit_cpu': '500m'
            }
        }
     },
     {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'api_production',
        'source_table': 'favorites',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'favorites',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '250m',
            'limit_memory': '2G',
            'limit_cpu': '1000m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'partners',  # small table with ~100 records
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'partners',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '250m',
            'limit_memory': '500M',
            'limit_cpu': '500m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'spree_countries',  # small table with ~250 records
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'spree_countries',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '250m',
            'limit_memory': '500M',
            'limit_cpu': '500m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'upcs',  # small table with ~700 records
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'upcs',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '250m',
            'limit_memory': '500M',
            'limit_cpu': '500m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'temp_infra_routing',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'temp_infra_routing',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '250m',
            'limit_memory': '1G',
            'limit_cpu': '500m'
            }
        }
     }
]
