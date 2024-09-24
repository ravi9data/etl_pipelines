config = [
    {
        'task_name': 'eu_conversion_adjustment',
        'sqlfile': './dags/reverse_etl/crm/sql/eu_conversion_adjustment.sql',
        'var_gsheet_id': 'eu_conversion_adjustment_gsheet',
        'sheet_name': 'Sheet1'
    },
    {
        'task_name': 'us_conversion_adjustment',
        'sqlfile': './dags/reverse_etl/crm/sql/us_conversion_adjustment.sql',
        'var_gsheet_id': 'us_conversion_adjustment_gsheet',
        'sheet_name': 'Sheet1'
    },
    {
        'task_name': 'us_offline_conversion_upload',
        'sqlfile': './dags/reverse_etl/crm/sql/us_offline_conversion_upload.sql',
        'var_gsheet_id': 'us_offline_conversion_upload_gsheet',
        'sheet_name': 'Sheet1'
    },
    {
        'task_name': 'us_offline_conversion_adjustment',
        'sqlfile': './dags/reverse_etl/crm/sql/us_offline_conversion_adjustment.sql',
        'var_gsheet_id': 'us_offline_conversion_adjustment_gsheet',
        'sheet_name': 'Sheet1'
    },
    {
        'task_name': 'eu_offline_conversion_upload',
        'sqlfile': './dags/reverse_etl/crm/sql/eu_offline_conversion_upload.sql',
        'var_gsheet_id': 'eu_offline_conversion_upload_gsheet',
        'sheet_name': 'Sheet1'
    },
    {
        'task_name': 'eu_offline_conversion_adjustment',
        'sqlfile': './dags/reverse_etl/crm/sql/eu_offline_conversion_adjustment.sql',
        'var_gsheet_id': 'eu_offline_conversion_adjustment_gsheet',
        'sheet_name': 'Sheet1'
    },
    {
        'task_name': 'eu_offline_new_customer_upload',
        'sqlfile': './dags/reverse_etl/crm/sql/eu_offline_new_customer_upload.sql',
        'var_gsheet_id': 'eu_offline_new_customer_upload_gsheet',
        'sheet_name': 'Sheet1'
    },
    {
        'task_name': 'us_offline_new_customer_upload',
        'sqlfile': './dags/reverse_etl/crm/sql/us_offline_new_customer_upload.sql',
        'var_gsheet_id': 'us_offline_new_customer_upload_gsheet',
        'sheet_name': 'Sheet1'
    },
    {
        'task_name': 'eu_offline_new_customer_adjustment',
        'sqlfile': './dags/reverse_etl/crm/sql/eu_offline_new_customer_adjustment.sql',
        'var_gsheet_id': 'eu_offline_new_customer_adjustment_gsheet',
        'sheet_name': 'Sheet1'
    },
    {
        'task_name': 'us_offline_new_customer_adjustment',
        'sqlfile': './dags/reverse_etl/crm/sql/us_offline_new_customer_adjustment.sql',
        'var_gsheet_id': 'us_offline_new_customer_adjustment_gsheet',
        'sheet_name': 'Sheet1'
    },
    {
        'task_name': 'bing_offline_conversion_upload',
        'sqlfile': './dags/reverse_etl/crm/sql/bing_offline_conversion_upload.sql',
        'var_gsheet_id': 'bing_offline_conversion_upload_gsheet',
        'sheet_name': 'Sheet1'
    },
    {
        'task_name': 'bing_offline_conversion_adjustment',
        'sqlfile': './dags/reverse_etl/crm/sql/bing_offline_conversion_adjustment.sql',
        'var_gsheet_id': 'bing_offline_conversion_adjustment_gsheet',
        'sheet_name': 'Sheet1'
    },
    {
        'task_name': 'eu_b2b_conversion_adjustment',
        'sqlfile': './dags/reverse_etl/crm/sql/eu_b2b_conversion_adjustment.sql',
        'var_gsheet_id': 'eu_b2b_conversion_adjustment_gsheet',
        'sheet_name': 'Sheet1'
    },
    {
        'task_name': 'us_b2b_conversion_adjustment',
        'sqlfile': './dags/reverse_etl/crm/sql/us_b2b_conversion_adjustment.sql',
        'var_gsheet_id': 'us_b2b_conversion_adjustment_gsheet',
        'sheet_name': 'Sheet1'
    }
]
