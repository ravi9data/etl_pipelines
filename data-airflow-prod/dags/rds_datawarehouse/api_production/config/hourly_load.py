config = [
    {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'applicants',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'applicants',
        'chunk_size': 25000,
        'max_rows_by_file': 25000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '250m',
            'limit_memory': '2G',
            'limit_cpu': '500m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'companies',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'companies',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '250m',
            'limit_memory': '2G',
            'limit_cpu': '500m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'api_production',
        'source_table': 'old_prices',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'old_prices',
        'chunk_size': 500000,
        'max_rows_by_file': 500000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1.5G',
            'request_cpu': '500m',
            'limit_memory': '6G',
            'limit_cpu': '1000m'
        }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'pictures',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'pictures',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '250m',
            'limit_memory': '3G',
            'limit_cpu': '500m'
        }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'product_store_ranks',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'product_store_ranks',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '250m',
            'limit_memory': '3G',
            'limit_cpu': '500m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'api_production',
        'source_table': 'rental_plans',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'rental_plans',
        'chunk_size': 500000,
        'max_rows_by_file': 500000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1.5G',
            'request_cpu': '500m',
            'limit_memory': '6G',
            'limit_cpu': '1000m'
        }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'api_production',
        'source_table': 'spree_line_items',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'spree_line_items',
        'chunk_size': 10000,
        'max_rows_by_file': 10000,
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500Mi',
            'request_cpu': '500m',
            'limit_memory': '6G',
            'limit_cpu': '2000m'
            }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'api_production',
        'source_table': 'spree_orders',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'spree_orders',
        'json_encoding': True,
        'chunk_size': 7000,
        'max_rows_by_file': 7000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500Mi',
            'request_cpu': '500m',
            'limit_memory': '6G',
            'limit_cpu': '1000m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'spree_products',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'spree_products',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '500m',
            'limit_memory': '3G',
            'limit_cpu': '500m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'spree_stores',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'spree_stores',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '250m',
            'limit_memory': '3G',
            'limit_cpu': '500m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'spree_variants',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'spree_variants',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '250m',
            'limit_memory': '3G',
            'limit_cpu': '500m'
            }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'api_production',
        'source_table': 'user_payment_methods',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'user_payment_methods',
        'json_encoding': True,
        'chunk_size': 10000,
        'max_rows_by_file': 10000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500Mi',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '2000m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'variant_stores',
        'target_s3_prefix': 'rds_datawarehouse/api_production/',
        'target_glue_table': 'variant_stores',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '250m',
            'limit_memory': '3G',
            'limit_cpu': '500m'
            }
        }
    }
]
