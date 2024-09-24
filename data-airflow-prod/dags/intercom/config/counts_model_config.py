config = [
    {
        'task_name': 'conversations_count',
        'url': 'https://api.intercom.io/counts?type=conversation',
        'glue_table_name': 'conversations_count',
        's3_bucket_prefix': 'counts_model/conversations_count',
        'response_filter1': 'conversation'
    },
    {
        'task_name': 'admin_count',
        'url': 'https://api.intercom.io/counts?type=conversation&count=admin',
        'glue_table_name': 'admin_count',
        's3_bucket_prefix': 'counts_model/admin_count',
        'response_filter1': 'conversation',
        'response_filter2': 'admin'
    },
    {
        'task_name': 'teams',
        'url': 'https://api.intercom.io/teams',
        'glue_table_name': 'teams',
        's3_bucket_prefix': 'counts_model/teams',
        'response_filter1': 'teams'
    }]
