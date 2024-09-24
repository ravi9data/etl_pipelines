import datetime

from plugins.dag_utils import on_failure_callback

default_args_braze_linear_projections = {
    "owner": 'data-eng',
    "depends_on_past": False,
    "start_date": datetime.datetime(2022, 1, 20),
    "retries": 3,
    "retry_delay": datetime.timedelta(seconds=15),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure_callback,
    "execution_timeout": datetime.timedelta(minutes=30)
}

list_of_tables = [
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_messages_email_send",
        "target_schema": "stg_external_apis",
        "target_table": "braze_sent_event",
        "partition_column": "date",
        "dag_schedule": "30 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, dispatch_id, external_user_id, "time", timezone,\
            campaign_id, campaign_name, message_variation_id, email_address, canvas_id,\
            canvas_name, canvas_variation_id, canvas_step_id, send_id, canvas_variation_name,\
            canvas_step_name, ip_pool, "date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_messages_email_open",
        "target_schema": "stg_external_apis",
        "target_table": "braze_open_event",
        "partition_column": "date",
        "dag_schedule": "30 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id ,user_id ,dispatch_id ,external_user_id ,"time" ,timezone ,\
            campaign_id ,campaign_name ,message_variation_id ,email_address ,canvas_id ,\
            canvas_name ,canvas_variation_id ,canvas_step_id ,send_id ,user_agent ,\
            canvas_variation_name ,canvas_step_name, ip_pool ,machine_open , "date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_messages_email_click",
        "target_schema": "stg_external_apis",
        "target_table": "braze_click_event",
        "partition_column": "date",
        "dag_schedule": "30 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, dispatch_id, external_user_id, "time", timezone,\
            campaign_id, campaign_name, message_variation_id, email_address, url,\
            canvas_id, canvas_name, canvas_variation_id, canvas_step_id, send_id,\
            user_agent, canvas_variation_name, canvas_step_name, ip_pool, link_id,\
            link_alias,  "date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_campaigns_conversion",
        "target_schema": "stg_external_apis",
        "target_table": "braze_campaign_conversion",
        "partition_column": "date",
        "dag_schedule": "30 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, external_user_id, app_id, "time", timezone,\
            campaign_id, campaign_name, message_variation_id, send_id,\
            conversion_behavior_index, conversion_behavior, "date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_canvas_conversion",
        "target_schema": "stg_external_apis",
        "target_table": "braze_canvas_conversion",
        "partition_column": "date",
        "dag_schedule": "30 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, external_user_id, app_id, "time", timezone,\
            canvas_id, canvas_name, canvas_variation_id, canvas_step_id,\
            conversion_behavior_index, conversion_behavior, canvas_variation_name,\
            canvas_step_name, "date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_messages_email_unsubscribe",
        "target_schema": "stg_external_apis",
        "target_table": "braze_unsubscribe_event",
        "partition_column": "date",
        "dag_schedule": "30 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, dispatch_id, external_user_id, "time", timezone,\
            campaign_id, campaign_name, message_variation_id, email_address, canvas_id,\
            canvas_name, canvas_variation_id, canvas_step_id, send_id,\
            canvas_variation_name, canvas_step_name, ip_pool, "date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_messages_email_delivery",
        "target_schema": "stg_external_apis",
        "target_table": "braze_delivery_event",
        "partition_column": "date",
        "dag_schedule": "30 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, dispatch_id, external_user_id, "time", timezone,\
            campaign_id, campaign_name, message_variation_id, email_address, canvas_id,\
            canvas_name, canvas_variation_id, canvas_step_id, send_id, sending_ip,\
            canvas_variation_name, canvas_step_name, ip_pool, "date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_canvas_entry",
        "target_schema": "stg_external_apis",
        "target_table": "braze_canvas_entry_event",
        "partition_column": "date",
        "dag_schedule": "30 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, external_user_id, "time", timezone, canvas_id,\
            canvas_name, canvas_variation_id, canvas_step_id, in_control_group,\
            canvas_variation_name, canvas_step_name, "date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_messages_email_bounce",
        "target_schema": "stg_external_apis",
        "target_table": "braze_email_bounce_event",
        "partition_column": "date",
        "dag_schedule": "30 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, dispatch_id, external_user_id, "time", timezone,\
            campaign_id, campaign_name, message_variation_id, email_address, canvas_id,\
            canvas_name, canvas_variation_id, canvas_step_id, send_id, sending_ip,\
            canvas_variation_name, canvas_step_name, ip_pool, bounce_reason, "date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_messages_email_markasspam",
        "target_schema": "stg_external_apis",
        "target_table": "braze_email_markasspam_event",
        "partition_column": "date",
        "dag_schedule": "30 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, dispatch_id, external_user_id, "time", timezone,\
            campaign_id, campaign_name, message_variation_id, email_address, canvas_id,\
            canvas_name, canvas_variation_id, canvas_step_id, send_id, user_agent,\
            canvas_variation_name, canvas_step_name, ip_pool, "date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_messages_email_softbounce",
        "target_schema": "stg_external_apis",
        "target_table": "braze_email_softbounce_event",
        "partition_column": "date",
        "dag_schedule": "30 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, dispatch_id, external_user_id, "time", timezone,\
            campaign_id, campaign_name, message_variation_id, email_address, canvas_id,\
            canvas_name, canvas_variation_id, canvas_step_id, send_id, sending_ip,\
            canvas_variation_name, canvas_step_name, ip_pool, bounce_reason, "date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_messages_contentcard_click",
        "target_schema": "stg_external_apis",
        "target_table": "braze_contentcard_click_event",
        "partition_column": "date",
        "dag_schedule": "40 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, external_user_id, content_card_id, "time", app_id,\
            send_id, campaign_id, campaign_name, message_variation_id, canvas_id,\
            canvas_variation_id, canvas_step_id, canvas_variation_name, canvas_step_name,\
            canvas_name, timezone, device_id, platform, os_version, device_model, ad_id,\
            ad_id_type, ad_tracking_enabled, "date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_messages_contentcard_impression",
        "target_schema": "stg_external_apis",
        "target_table": "braze_contentcard_impression_event",
        "partition_column": "date",
        "dag_schedule": "40 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, external_user_id, content_card_id, "time", app_id, send_id,\
            campaign_id, campaign_name, message_variation_id, canvas_id, canvas_variation_id,\
            canvas_variation_name, canvas_step_id, canvas_step_name, canvas_name, timezone,\
            device_id, platform, os_version, device_model, ad_id, ad_id_type, ad_tracking_enabled,\
            "date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_messages_contentcard_send",
        "target_schema": "stg_external_apis",
        "target_table": "braze_contentcard_send_event",
        "partition_column": "date",
        "dag_schedule": "40 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, external_user_id, content_card_id, "time", send_id, campaign_id,\
            campaign_name, message_variation_id, canvas_id, canvas_variation_id,\
            canvas_variation_name, canvas_step_id, canvas_step_name, canvas_name,\
            timezone, device_id,"date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_messages_inappmessage_click",
        "target_schema": "stg_external_apis",
        "target_table": "braze_inappmessage_click_event",
        "partition_column": "date",
        "dag_schedule": "40 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, external_user_id, app_id, "time", timezone, campaign_id,\
            campaign_name, message_variation_id, canvas_id, canvas_name, canvas_variation_id,\
            canvas_step_id, card_id, platform, os_version, device_model, button_id, send_id,\
            device_id, canvas_variation_name, canvas_step_name, ad_id, ad_id_type,\
            ad_tracking_enabled, "date"'
    },
    {
        "source_schema": "s3_spectrum_braze_currents",
        "source_table": "event_type_users_messages_inappmessage_impression",
        "target_schema": "stg_external_apis",
        "target_table": "braze_inappmessage_impression_event",
        "partition_column": "date",
        "dag_schedule": "40 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        "column_list": 'id, user_id, external_user_id, "time", timezone, app_id, campaign_id,\
            campaign_name, message_variation_id, canvas_id, canvas_name, canvas_variation_id,\
            canvas_step_id, card_id, platform, os_version, device_model, send_id, device_id,\
            ad_id, ad_id_type, ad_tracking_enabled, canvas_variation_name, canvas_step_name, "date"'
    },

    {
        'source_schema': 's3_spectrum_braze_currents',
        'source_table': 'event_type_users_messages_pushnotification_bounce',
        'target_schema': 'stg_external_apis',
        'target_table': 'braze_pushnotification_bounce_event',
        'partition_column': 'date',
        'dag_schedule': "40 * * * *",
        'dist_key': 'id', 'sort_key': 'id',
        'column_list': 'id, user_id, external_user_id, app_id, "time", timezone, platform,\
            campaign_id, campaign_name, message_variation_id, canvas_id, canvas_name,\
            canvas_variation_id, canvas_step_id, send_id, dispatch_id, device_id,\
            canvas_variation_name, canvas_step_name, ad_id, ad_id_type, ad_tracking_enabled, "date"'
    },
    {
        'source_schema': 's3_spectrum_braze_currents',
        'source_table': 'event_type_users_messages_pushnotification_open',
        'target_schema': 'stg_external_apis',
        'target_table': 'braze_pushnotification_open_event',
        'partition_column': 'date',
        'dag_schedule': "40 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        'column_list': 'id, user_id, external_user_id, "time", timezone, app_id, campaign_id,\
            campaign_name, message_variation_id, canvas_id, canvas_name, canvas_variation_id,\
            canvas_variation_name, canvas_step_id, canvas_step_name,\
            canvas_step_message_variation_id,platform, os_version, device_model, send_id,\
            dispatch_id, device_id, button_action_type,button_string, ad_id, ad_id_type,\
            ad_tracking_enabled, "date"'
    },
    {
        'source_schema': 's3_spectrum_braze_currents',
        'source_table': 'event_type_users_messages_pushnotification_send',
        'target_schema': 'stg_external_apis',
        'target_table': 'braze_pushnotification_send_event',
        'partition_column': 'date',
        'dag_schedule': "40 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        'column_list': 'id, user_id, external_user_id, app_id, "time", timezone, platform, campaign_id,\
            campaign_name, message_variation_id, canvas_id, canvas_name, canvas_variation_id,\
            canvas_step_id, send_id, dispatch_id, device_id, canvas_variation_name,\
            canvas_step_name, ad_id, ad_id_type, ad_tracking_enabled, "date"'
    },
    {
        'source_schema': 's3_spectrum_braze_currents',
        'source_table': 'event_type_users_behaviors_app_newsfeedimpression',
        'target_schema': 'stg_external_apis',
        'target_table': 'braze_users_behaviors_app_newsfeedimpression',
        'partition_column': 'date',
        'dag_schedule': "40 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        'column_list': 'id, user_id, external_user_id, app_id, "time", platform, os_version, device_model,\
            device_id, "date"'
    },
    {
        'source_schema': 's3_spectrum_braze_currents',
        'source_table': 'event_type_users_messages_webhook_send',
        'target_schema': 'stg_external_apis',
        'target_table': 'braze_users_messages_webhook_send',
        'partition_column': 'date',
        'dag_schedule': "40 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        'column_list': 'id, user_id, external_user_id, "time", timezone, campaign_id, campaign_name,\
            message_variation_id, canvas_id, canvas_name, canvas_variation_id, canvas_step_id,\
            send_id,canvas_variation_name, canvas_step_name, "date"'
    },
    {
        'source_schema': 's3_spectrum_braze_currents',
        'source_table': 'event_type_users_behaviors_uninstall',
        'target_schema': 'stg_external_apis',
        'target_table': 'braze_users_behaviors_uninstall',
        'partition_column': 'date',
        'dag_schedule': "40 * * * *",
        "dist_key": 'id',
        "sort_key": 'id',
        'column_list': 'id, user_id, external_user_id, app_id, "time", device_id, "date"'
    }
]
