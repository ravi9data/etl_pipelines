datasets_config = [
    {
        "unload_task_id": "product_unload",
        "source_schema": "ods_production",
        "source_table": "product",
        "sql_query": "SELECT product_id, product_sku, category_name,subcategory_name,\"rank\","
                     "to_char(created_at, ''YYYY-MM-DD HH24:MI:SS.MS'') AS created_at,"
                     "to_char(updated_at, ''YYYY-MM-DD HH24:MI:SS.MS'') AS updated_at,   "
                     "to_char(current_timestamp, ''YYYY-MM-DD HH24:MI:SS.MS'') AS extracted_at "
                     "FROM ods_production.product",
        "redshift_datasets_s3_bucket": "grover-eu-central-1-production-data-redshift-datasets",
        "s3_prefix": "ods_production/product/unload_",
        "unload_option": ["PARALLEL FALSE", "ALLOWOVERWRITE", "PARQUET"]
    },
    {
        "unload_task_id": "asset_payment_unload",
        "source_schema": "master",
        "source_table": "v_master_asset_payment",
        "sql_query": "SELECT * FROM master.v_master_asset_payment",
        "redshift_datasets_s3_bucket": "grover-eu-central-1-production-data-redshift-datasets",
        "s3_prefix": "master/asset_payment/unload_",
        "unload_option": ["PARALLEL FALSE", "ALLOWOVERWRITE", "PARQUET"]
    },
    {
        "unload_task_id": "subscription_unload",
        "source_schema": "master",
        "source_table": "v_master_subscription",
        "sql_query": "SELECT * FROM master.v_master_subscription",
        "redshift_datasets_s3_bucket": "grover-eu-central-1-production-data-redshift-datasets",
        "s3_prefix": "master/subscription/unload_",
        "unload_option": ["PARALLEL FALSE", "ALLOWOVERWRITE", "PARQUET"]
    },
    {
        "unload_task_id": "subscription_payment_unload",
        "source_schema": "master",
        "source_table": "v_master_subscription_payment",
        "sql_query": "SELECT * FROM master.v_master_subscription_payment",
        "redshift_datasets_s3_bucket": "grover-eu-central-1-production-data-redshift-datasets",
        "s3_prefix": "master/subscription_payment/unload_",
        "unload_option": ["PARALLEL FALSE", "ALLOWOVERWRITE", "PARQUET"]
    },
    {
        "unload_task_id": "grover_market_value_unload",
        "source_schema": "ods_production",
        "source_table": "grover_market_value",
        "sql_query": "SELECT * FROM ods_production.grover_market_value",
        "redshift_datasets_s3_bucket": "grover-eu-central-1-production-data-redshift-datasets",
        "s3_prefix": "ods_production/grover_market_value/unload_",
        "unload_option": ["ALLOWOVERWRITE", "DELIMITER AS ','", "GZIP", "PARALLEL OFF", "ESCAPE"]
    },
    {
        "unload_task_id": "grover_market_value_historical_unload",
        "source_schema": "master",
        "source_table": "v_ods_grover_market_value_historical",
        "sql_query": "SELECT * FROM ods_production.v_ods_grover_market_value_historical",
        "redshift_datasets_s3_bucket": "grover-eu-central-1-production-data-redshift-datasets",
        "s3_prefix": "ods_production/grover_market_value_historical/unload_",
        "unload_option": ["ALLOWOVERWRITE", "DELIMITER AS ','", "GZIP", "PARALLEL OFF", "ESCAPE"]
    },
    {
        "unload_task_id": "grover_market_value_historical_un_compressed_unload",
        "source_schema": "master",
        "source_table": "v_ods_grover_market_value_historical",
        "sql_query": "SELECT * FROM ods_production.v_ods_grover_market_value_historical",
        "redshift_datasets_s3_bucket": "grover-eu-central-1-production-data-redshift-datasets",
        "s3_prefix": "ods_production/grover_market_value_historical/un_compressed/unload_",
        "unload_option": ["PARALLEL FALSE", "ALLOWOVERWRITE", "PARQUET"]
    }
]
