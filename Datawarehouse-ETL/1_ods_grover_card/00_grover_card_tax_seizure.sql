
-----------Grover Card Tax_id
select distinct JSON_EXTRACT_PATH_text(payload,'user_id')::int as customer_id,
		JSON_EXTRACT_PATH_text(payload,'external_user_id') as external_user_id,
		JSON_EXTRACT_PATH_text(payload,'date')::date as tax_id_submitted_date	

---------Grover Card Seizures
SELECT 
	json_extract_path_text(payload,'creditor_representative') AS creditor_representative,
	json_extract_path_text(payload,'user_id')::int AS customer_id,
	regexp_replace(replace(replace(json_extract_path_text(payload,'amount'),'.',''),',','.'),'([^0-9.])')::DECIMAL(38,2) AS amount,
	json_extract_path_text(payload,'seizure_type')AS seizure_type,
	json_extract_path_text(payload,'date')::date AS date
WHERE event_name='account-seizure';

