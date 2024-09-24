config = [
     {
        'is_full_load': True,
        'source_schema': 'order_approval',
        'source_table': 'boniversum_data',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'boniversum_data',
        'chunk_size': 25000,
        'max_rows_by_file': 25000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '250m',
            'limit_memory': '5G',
            'limit_cpu': '500m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'order_approval',
        'source_table': 'burgel_data',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'burgel_data',
        'chunk_size': 25000,
        'max_rows_by_file': 25000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '2G',
            'request_cpu': '2000m',
            'limit_memory': '5G',
            'limit_cpu': '3500m'
            }
        }
     },
     {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'order_approval',
        'source_table': 'crifburgel_data',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'crifburgel_data',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '1500m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'order_approval',
        'source_table': 'customer_labels_state_changes',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'customer_labels_state_changes',
        'chunk_size': 25000,
        'max_rows_by_file': 25000,
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '1000m'
            }
        }
     },
     {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'order_approval',
        'source_table': 'decision_result',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'decision_result',
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
        'source_schema': 'order_approval',
        'source_table': 'experian_es_delphi_data',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'experian_es_delphi_data',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '2G',
            'request_cpu': '2000m',
            'limit_memory': '5G',
            'limit_cpu': '3500m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'order_approval',
        'source_table': 'id_verification_order',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'id_verification_order',
        'json_encoding': True,
        'chunk_size': 7000,
        'max_rows_by_file': 7000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '2G',
            'request_cpu': '2000m',
            'limit_memory': '5G',
            'limit_cpu': '3500m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'order_approval',
        'source_table': 'mix_log',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'mix_log',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '1000m'
            }
        }
     },
     {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'order_approval',
        'source_table': 'order_item',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'order_item',
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
        'source_schema': 'order_approval',
        'source_table': 'order_payment',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'order_payment',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '1000m',
            'limit_memory': '5G',
            'limit_cpu': '1500m'
            }
        }
     },
     {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'order_approval',
        'source_table': 'order_similarity_result',
        'source_uid_column': 'order_id',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'order_similarity_result',
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
        'source_schema': 'order_approval',
        'source_table': 'recurring_customer_new_flow',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'recurring_customer_new_flow',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '1000m',
            'limit_memory': '5G',
            'limit_cpu': '2000m'
            }
        }
     },
     {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'order_approval',
        'source_table': 'schufa_data',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'schufa_data',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '1000m'
            }
        }
     },
     {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'order_approval',
        'source_table': 'seon_phone_data',
        'source_uid_column': 'id',
        'source_columns': [
            'created_at',
            'updated_at',
            'id',
            'customer_id',
            'seon_id',
            'seon_transaction_id',
            'seon_transaction_state',
            'is_valid',
            'phone_score',
            'phone_number',
            'phone_type',
            'carrier',
            'registered_country',
            'fraud_score',
            'order_id',
            'phone_accounts',
            'phone_names_dict'
        ],
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'seon_phone_data',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500m',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '1000m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'order_approval',
        'source_table': 'equifax_risk_score_data',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'equifax_risk_score_data',
        'json_encoding': True,
        # setup the chunk_size to avoid memory blowing up
        'chunk_size': 10000,
        'max_rows_by_file': 10000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '750M',
            'request_cpu': '1000m',
            'limit_memory': '5G',
            'limit_cpu': '2000m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'order_approval',
        'source_table': 'focum_data',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'focum_data',
        'json_encoding': True,
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
        'source_timestamp_field': 'creation_timestamp',
        'source_schema': 'order_approval',
        'source_table': 'decision_tree_algorithm_results',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'decision_tree_algorithm_results',
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
        'source_schema': 'order_approval',
        'source_table': 'spree_users_subscription_limit_log',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'spree_users_subscription_limit_log',
        'chunk_size': 25000,
        'max_rows_by_file': 25000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '1000m'
            }
        }
     },
     {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'order_approval',
        'source_table': 'nethone_data',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'nethone_data',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '250M',
            'request_cpu': '500m',
            'limit_memory': '5G',
            'limit_cpu': '1500m'
            }
        }
     },
     {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'order_approval',
        'source_table': 'seon_email_data',
        'source_uid_column': 'id',
        'source_columns': [
            'created_at',
            'updated_at',
            'id',
            'customer_id',
            'seon_id',
            'seon_transaction_id',
            'seon_transaction_state',
            'fraud_score',
            'email_breached',
            'email_score',
            'email_address',
            'order_id',
            'email_accounts',
            'email_names_dict'
        ],
        'target_s3_prefix': 'rds_datawarehouse/order_approval/',
        'target_glue_table': 'seon_email_data',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '750m',
            'limit_memory': '5G',
            'limit_cpu': '1000m'
            }
        }
     }
]
