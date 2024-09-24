-----Card Account Closure

	SELECT 
	JSON_EXTRACT_PATH_text(payload,'user_id')::int as customer_id,
	JSON_EXTRACT_PATH_text(payload,'status') as status,
	JSON_EXTRACT_PATH_text(payload,'closure_reason') as closure_reason,
	JSON_EXTRACT_PATH_text(payload,'legal_closure_date')::timestamp as legal_closure_date,
	nullif(JSON_EXTRACT_PATH_text(payload,'technical_closure_date'),'')::timestamp as technical_closure_date
	WHERE payload NOT LIKE '%key%'
	AND payload <>'null'
	union
	SELECT 
	json_extract_path_text(json_extract_path_text(payload, 'value'),'user_id') ::int as customer_id,
	json_extract_path_text(json_extract_path_text(payload, 'value'),'status')  as status,
	json_extract_path_text(json_extract_path_text(payload, 'value'),'closure_reason')  as closure_reason,
	json_extract_path_text(json_extract_path_text(payload, 'value'),'legal_closure_date') ::timestamp as legal_closure_date,
	json_extract_path_text(json_extract_path_text(payload, 'value'),'technical_closure_date') ::timestamp as technical_closure_date
	WHERE payload LIKE '%key%';

