begin transaction;
insert into atomic.com_snowplowanalytics_snowplow_web_page_1
SELECT 
	'com.snowplowanalytics.snowplow' as schema_vendor, 
	'web_page' as schema_name, 
	'jsonschema' as schema_format,
	'1-0-0' as schema_version, 
	event_id as root_id, 
	collector_tstamp::timestamp as root_tstamp, 
	'events' as ref_root, 
	'["events","web_page"]' as ref_tree, 
	'events' as ref_parent,
     json_extract_path_text(
        json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_web_page_1, 0)
    	,'id') as id
FROM stg_kafka_events.stream_snowplow_event_parser
where id is not null;
end transaction;