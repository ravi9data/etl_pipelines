from business_logic.manual_review_us.data_preparation.flattening_data import (
    flatten_flex_log, flatten_flex_order, flatten_nethone_data,
    flatten_order_payment_method, flatten_precise_data)

list_of_tables = [
    {
        "glue_table_name": "risk_internal_us_risk_flex_order_v1",
        "glue_db_varname": "glue_db_kafka",
        "columns_name": [
            "created_at",
            "order_id",
            "customer_id",
            "raw_data",
            "updated_at"],
        "dataframe_name": "flex_order",
        "python_flattening_func": flatten_flex_order,
        "column_to_flatten": "raw_data",
        "customer_based": False
    },
    {
        "glue_table_name": "risk_internal_us_risk_flex_log_v1",
        "glue_db_varname": "glue_db_kafka",
        "columns_name": [
            "order_id",
            "raw_data",
            "event_name"],
        "dataframe_name": "flex_log",
        "python_flattening_func": flatten_flex_log,
        "column_to_flatten": "raw_data",
        "event_name_to_filter": "scoring.customer_fraud_check_initiated",
        "customer_based": False
    },
    {
        "glue_table_name": "risk_internal_us_risk_order_payment_method_v1",
        "glue_db_varname": "glue_db_kafka",
        "columns_name": [
            "order_id",
            "payment_type",
            "payment_metadata"
            ],
        "dataframe_name": "order_payment_method",
        "python_flattening_func": flatten_order_payment_method,
        "column_to_flatten": "payment_metadata",
        "customer_based": False
    },
    {
        "glue_table_name": "risk_internal_us_risk_decision_result_v1",
        "glue_db_varname": "glue_db_kafka",
        "columns_name": [
            "order_id",
            "code",
            "amount",
            "message"],
        "dataframe_name": "risk_decision_result",
        "column_to_flatten": None,
        "customer_based": False
    },
    {
        "glue_table_name": "risk_internal_us_risk_nethone_data_v1",
        "glue_db_varname": "glue_db_kafka",
        "columns_name": [
            "customer_id",
            "updated_at",
            "data"],
        "dataframe_name": "risk_nethone_data",
        "python_flattening_func": flatten_nethone_data,
        "column_to_flatten": "data",
        "customer_based": True
    },
    {
        "glue_table_name": "risk_internal_us_risk_order_status_v1",
        "glue_db_varname": "glue_db_kafka",
        "columns_name": [
            "order_id",
            "status"],
        "dataframe_name": "risk_order_status",
        "column_to_flatten": None,
        "customer_based": False
    },
    {
        "glue_table_name": "risk_internal_us_risk_seon_email_data_v1",
        "glue_db_varname": "glue_db_kafka",
        "columns_name": [
            "customer_id",
            "updated_at",
            "email_address",
            "seon_transaction_id"],
        "dataframe_name": "seon_email",
        "columns_to_flatten": None,
        "customer_based": True
    },
    {
        "glue_table_name": "risk_internal_us_risk_ekata_us_transaction_risk_data_v1",
        "glue_db_varname": "glue_db_kafka",
        "columns_name": [
            "customer_id",
            "identity_network_score",
            "identity_risk_score",
            "primary_address_first_seen_days",
            "primary_address_to_name",
            "primary_address_validity_level",
            "primary_email_first_seen_days",
            "primary_email_to_name",
            "primary_phone_line_type",
            "primary_phone_to_name"],
        "dataframe_name": "ekata_us_transaction",
        "column_to_flatten": None,
        "customer_based": True
    },
    {
        "glue_table_name": "risk_internal_us_risk_experian_us_credit_report_data_v1",
        "glue_db_varname": "glue_db_kafka_sensitive",
        "columns_name": [
            "customer_id",
            "score",
            "features"],
        "dataframe_name": "experian_us_credit",
        "column_to_flatten": None,
        "customer_based": True
    },
    {
        "glue_table_name": "risk_internal_us_risk_experian_us_precise_id_data_v1",
        "glue_db_varname": "glue_db_kafka_sensitive",
        "columns_name": [
            "customer_id",
            "fpdscore",
            "precise_id_score",
            "raw_precise_data"],
        "dataframe_name": "experian_us_precise",
        "python_flattening_func": flatten_precise_data,
        "column_to_flatten": "raw_precise_data",
        "customer_based": True
    }
]

empty_columns_list = [
    'overlimit',
    'failed_payments',
    'reviewer feedback',
    'issues',
    'onfido_current_state',
    'ssn_id'
]
