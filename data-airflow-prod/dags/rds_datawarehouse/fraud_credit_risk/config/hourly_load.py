config = [
    {
        'is_full_load': True,
        'source_schema': 'fraud_and_credit_risk',
        'source_table': 'anomaly_score_daily',
        'target_s3_prefix': 'rds_datawarehouse/fraud_and_credit_risk/',
        'target_glue_table': 'anomaly_score_daily',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '4000m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'fraud_and_credit_risk',
        'source_table': 'anomaly_score_order',
        'target_s3_prefix': 'rds_datawarehouse/fraud_and_credit_risk/',
        'target_glue_table': 'anomaly_score_order',
        'chunk_size': 25000,
        'max_rows_by_file': 25000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '1000m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'fraud_and_credit_risk',
        'source_table': 'asset_risk_categories_order',
        'target_s3_prefix': 'rds_datawarehouse/fraud_and_credit_risk/',
        'target_glue_table': 'asset_risk_categories_order',
        'chunk_size': 25000,
        'max_rows_by_file': 25000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '250m',
            'limit_memory': '5G',
            'limit_cpu': '1000m'
            }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'fraud_and_credit_risk',
        'source_table': 'customer_data',
        'source_uid_column': 'order_id',
        'target_s3_prefix': 'rds_datawarehouse/fraud_and_credit_risk/',
        'target_glue_table': 'customer_data',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '1000m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'fraud_and_credit_risk',
        'source_table': 'customer_tags',
        'target_s3_prefix': 'rds_datawarehouse/fraud_and_credit_risk/',
        'target_glue_table': 'customer_tags',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '1000m'
            }
        }
    },
    # {
    #     'is_full_load': False,
    #     'source_timestamp_field': 'created_at',
    #     'source_schema': 'fraud_and_credit_risk',
    #     'source_table': 'nethone_signal_risk_categories_order',
    #     'source_uid_column': 'id',
    #     'target_s3_prefix': 'rds_datawarehouse/fraud_and_credit_risk/',
    #     'target_glue_table': 'nethone_signal_risk_categories_order',
    #     'executor_config': {'KubernetesExecutor': {
    #         'request_memory': '250M',
    #         'request_cpu': '250m',
    #         'limit_memory': '1G',
    #         'limit_cpu': '1000m'
    #         }
    #     }
    # },
    {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'fraud_and_credit_risk',
        'source_table': 'order_approval_results',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/fraud_and_credit_risk/',
        'target_glue_table': 'order_approval_results',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '1000m',
            'limit_memory': '5G',
            'limit_cpu': '2000m'
            }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'fraud_and_credit_risk',
        'source_table': 'order_predictions',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/fraud_and_credit_risk/',
        'target_glue_table': 'order_predictions',
        'chunk_size': 10000,
        'max_rows_by_file': 10000,
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '10G',
            'request_cpu': '4000m',
            'limit_memory': '10G',
            'limit_cpu': '4000m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'fraud_and_credit_risk',
        'source_table': 'payment_risk_categories',
        'target_s3_prefix': 'rds_datawarehouse/fraud_and_credit_risk/',
        'target_glue_table': 'payment_risk_categories',
        'chunk_size': 25000,
        'max_rows_by_file': 25000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '1000m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'fraud_and_credit_risk',
        'source_table': 'pipedrive_company',
        'target_s3_prefix': 'rds_datawarehouse/fraud_and_credit_risk/',
        'target_glue_table': 'pipedrive_company',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '1000m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'fraud_and_credit_risk',
        'source_table': 'prediction_models',
        'target_s3_prefix': 'rds_datawarehouse/fraud_and_credit_risk/',
        'target_glue_table': 'prediction_models',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '250m',
            'limit_memory': '5G',
            'limit_cpu': '1000m'
            }
        }
    },
    {
        'is_full_load': True,
        'source_schema': 'fraud_and_credit_risk',
        'source_table': 'tags',
        'target_s3_prefix': 'rds_datawarehouse/fraud_and_credit_risk/',
        'target_glue_table': 'tags',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '250m',
            'limit_memory': '5G',
            'limit_cpu': '1000m'
            }
        }
    },
    {
        'is_full_load': False,
        'source_timestamp_field': 'order_submitted_at',
        'source_schema': 'fraud_and_credit_risk',
        'source_table': 'order_device_data',
        'source_uid_column': 'order_id',
        'target_s3_prefix': 'rds_datawarehouse/fraud_and_credit_risk/',
        'target_glue_table': 'order_device_data',
        'chunk_size': 10000,
        'max_rows_by_file': 10000,
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '3G',
            'request_cpu': '3000m',
            'limit_memory': '5G',
            'limit_cpu': '4000m'
            }
        }
    }
]
