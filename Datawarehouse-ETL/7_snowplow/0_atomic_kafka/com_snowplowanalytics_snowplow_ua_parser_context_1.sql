begin transaction;
insert into atomic.com_snowplowanalytics_snowplow_ua_parser_context_1 --this table need to change once production payload is there.
SELECT 
	'com.snowplowanalytics.snowplow' as schema_vendor, 
	'ua_parser_context' as schema_name, 
	'jsonschema' as schema_format,
	'1-0-0' as schema_version, 
	event_id as root_id, 
	collector_tstamp::timestamp as root_tstamp, 
	'events' as ref_root, 
	'["events","ua_parser_context"]' as ref_tree, 
	'events' as ref_parent,
    coalesce(json_extract_path_text(
    json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1, 0)
    	,'useragentFamily'),'N/A' ) as useragent_family,
      json_extract_path_text(
        json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1, 0)
    	,'useragentMajor') as useragent_major,
    	      json_extract_path_text(
        json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1, 0)
    	,'useragentMinor') as useragent_minor,
    	      json_extract_path_text(
        json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1, 0)
    	,'useragentPatch') as useragent_patch,
    	 coalesce(json_extract_path_text(
        json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1, 0)
    	,'useragentVersion'),'N/A' ) as useragent_version,
    	 coalesce(json_extract_path_text(
        json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1, 0)
    	,'osFamily'),'N/A' ) as os_family,
    	   	      json_extract_path_text(
        json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1, 0)
    	,'osMajor') as os_major,
    	   	      json_extract_path_text(
        json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1, 0)
    	,'osMinor') as os_minor,
    	   	      json_extract_path_text(
        json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1, 0)
    	,'osPatch') as os_patch,
    	   	      json_extract_path_text(
        json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1, 0)
    	,'osPatchMinor') as os_patch_minor,
    	coalesce(json_extract_path_text(json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1, 0)
    	,'osVersion'),'N/A' ) as os_version,
    	coalesce(json_extract_path_text(
        json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1, 0)
    	,'deviceFamily'),'N/A' ) as device_family
FROM stg_kafka_events.stream_snowplow_event_parser;
--this table need to change once production payload is there.
end transaction;
