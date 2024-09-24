config = {
   "segment_events": {
      "directory": "30_segment_events",
      "script_name": [
        "01_track_events_incremental",
        "02_page_events_incremental",
        "03_identify_events_incremental",
        "04_screen_events_incremental",
        "05_all_events_incremental"
        ]
   },
   "segment_page_view_and_sessions": {
      "directory": "31_segment_page_view_and_sessions",
      "script_name": [
        "06_00_customer_mapping",
        "06_01_url_store_mapping",
        "06_02_device_mapping_incremental",
        "06_03_consent_mapping_incremental",
        "06_04_snowplow_user_mapping",
        "06_11_page_views_app_incremental",
        "06_page_views_incremental",
        "07_session_marketing_mapping_incremental",
        "08_11_sessions_app_incremental",
        "08_sessions_incremental",
        "09_00_order_event_mapping",
        "09_01_order_user_mapping",
        "14_session_order_mapping_incremental",
        "15_order_conversions_incremental"
        ]
    }
}
