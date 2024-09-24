config = [
     {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'api_production',
        'source_table': 'spree_addresses',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/api_production_sensitive/',
        'target_glue_table': 'spree_addresses',
        'chunk_size': 10000,
        'max_rows_by_file': 10000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '3G',
            'request_cpu': '500m',
            'limit_memory': '6G',
            'limit_cpu': '2000m'
            }
        },
        'json_encoding': True
     },
     {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'employees',
        'target_s3_prefix': 'rds_datawarehouse/api_production_sensitive/',
        'target_glue_table': 'employees',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '2G',
            'request_cpu': '2000m',
            'limit_memory': '3G',
            'limit_cpu': '3000m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'personal_identifications',
        'target_s3_prefix': 'rds_datawarehouse/api_production_sensitive/',
        'target_glue_table': 'personal_identifications',
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '500M',
            'request_cpu': '250m',
            'limit_memory': '3G',
            'limit_cpu': '2000m'
            }
        }
     },
     {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'api_production',
        'source_table': 'adyen_payment_requests',
        'source_uid_column': 'id',
        'target_s3_prefix': 'rds_datawarehouse/api_production_sensitive/',
        'target_glue_table': 'adyen_payment_requests',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '3G',
            'request_cpu': '1000m',
            'limit_memory': '5G',
            'limit_cpu': '2000m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'checks',
        'target_s3_prefix': 'rds_datawarehouse/api_production_sensitive/',
        'target_glue_table': 'checks',
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '500m',
            'limit_memory': '3G',
            'limit_cpu': '3000m'
            }
        }
     },
     {
        'is_full_load': True,
        'source_schema': 'api_production',
        'source_table': 'waiting_list_entries',
        'target_s3_prefix': 'rds_datawarehouse/api_production_sensitive/',
        'target_glue_table': 'waiting_list_entries',
        'chunk_size': 25000,
        'max_rows_by_file': 25000,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '750M',
            'request_cpu': '500m',
            'limit_memory': '4G',
            'limit_cpu': '3000m'
            }
        }
     },
     {
        'is_full_load': False,
        'source_timestamp_field': 'updated_at',
        'source_schema': 'api_production',
        'source_table': 'spree_users',
        'source_uid_column': 'id',
        'source_columns': [
            'id',
            'email',
            'sign_in_count',
            'failed_attempts',
            'last_request_at',
            'current_sign_in_at',
            'last_sign_in_at',
            'current_sign_in_ip',
            'last_sign_in_ip',
            'login',
            'ship_address_id',
            'bill_address_id',
            'locked_at',
            'created_at',
            'updated_at',
            'spree_api_key',
            'remember_created_at',
            'deleted_at',
            'confirmed_at',
            'confirmation_sent_at',
            'birthdate::varchar',
            'stripe_customer_id',
            'status',
            'authy_id',
            'address_verification_response',
            'verified',
            'identified',
            'gender',
            'accept_newsletter',
            'store_id',
            'sift_score',
            'confirmation_resent_at',
            'deactivated_notification_sent_at',
            'default_locale',
            'curr_address_id',
            'delete_requested_at',
            'delete_approved_at',
            'delete_reason',
            'first_name',
            'last_name',
            'phone_number',
            'sift_label',
            'sift_salesforce_fraud_decision_data',
            'sift_label_response_data',
            'signup_language',
            'subscribed_to_newsletter',
            'phone_number_verified_at',
            'preferences',
            'subscription_limit',
            'trust_type',
            'address_legal_accepted',
            'payment_legal_accepted',
            'subscription_limit_change_date',
            'company_name',
            'user_type',
            'company_id',
            'mailchimp_status',
            'signup_country_id',
            'referral_code',
            'verification_state',
            'password_locked'
        ],
        'target_s3_prefix': 'rds_datawarehouse/api_production_sensitive/',
        'target_glue_table': 'spree_users',
        'chunk_size': 15000,
        'max_rows_by_file': 15000,
        'json_encoding': True,
        'executor_config': {'KubernetesExecutor': {
            'request_memory': '1G',
            'request_cpu': '500m',
            'limit_memory': '4G',
            'limit_cpu': '2000m'
            }
        }
     }
]
