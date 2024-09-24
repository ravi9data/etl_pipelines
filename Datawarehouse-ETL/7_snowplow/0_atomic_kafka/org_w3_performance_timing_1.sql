begin transaction;
insert into atomic.org_w3_performance_timing_1
SELECT 
	'org.w3' as schema_vendor, 
	'PerformanceTiming' as schema_name, 
	'jsonschema' as schema_format,
	'1-0-0' as schema_version, 
	event_id as root_id, 
	collector_tstamp::timestamp as root_tstamp, 
	'events' as ref_root, 
	'["events","PerformanceTiming"]' as ref_tree, 
	'events' as ref_parent,
     json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'navigationStart')::bigint as navigation_start,
      json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'redirectStart')::bigint as redirect_start,
    	      json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'redirectEnd')::bigint as redirect_end,
    	      json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'fetchStart')::bigint as fetch_start,
    	      json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'domainLookupStart')::bigint as domain_lookup_start,
    	      json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'domainLookupEnd')::bigint as domain_lookup_end,
    	case when json_extract_path_text(json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
		,'secureConnectionStart')='' then null else json_extract_path_text(json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'secureConnectionStart')::bigint end as secureConnectionStart,
    	   	      json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'connectStart')::bigint as connect_start,
    	   	      json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'connectEnd')::bigint as connect_end,
    	   	      json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'requestStart')::bigint as request_start,
    	   	      json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'responseStart')::bigint as response_start,
    	  		 json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'responseEnd')::bigint as response_end,
    	json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'unloadEventStart')::bigint as unload_event_start,
    	json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'unloadEventEnd')::bigint as unload_event_end,    	
    	json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'domLoading')::bigint as dom_loading,    	
    	json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'domInteractive')::bigint as dom_interactive,    	
    	json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'domContentLoadedEventStart')::bigint as dom_content_loaded_event_start,    	
    	json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'domContentLoadedEventEnd')::bigint as dom_content_loaded_event_end,    	
    	json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'domComplete')::bigint as dom_complete,    	
    	json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'loadEventStart')::bigint as load_event_start,    	
    	json_extract_path_text(
        json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'loadEventEnd')::bigint as load_event_end,    	
        case when json_extract_path_text(json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
				,'msFirstPaint')='' then null else json_extract_path_text(json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'msFirstPaint')::bigint end as ms_first_paint,
    	case when json_extract_path_text(json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
				,'chromeFirstPaint')='' then null else json_extract_path_text(json_extract_array_element_text(contexts_org_w3_performance_timing_1, 0)
    	,'chromeFirstPaint')::bigint end as ms_first_paint
FROM stg_kafka_events.stream_snowplow_event_parser
WHERE contexts_org_w3_performance_timing_1 <> '[{}]';
end transaction;