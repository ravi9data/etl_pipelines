delete from  staging.customers_contracts
where  consumed_at > current_date-2;

insert into staging.customers_contracts
select
	json_extract_path_text(payload ,'id') as contract_id,
	json_extract_path_text(payload ,'type') as "type",
	json_extract_path_text(payload ,'user_id') as customer_id,
	json_extract_path_text(payload ,'company_id') as company_id,
	json_extract_path_text(payload ,'order_number') as order_number ,
	json_extract_path_text(payload ,'billing_account_id') as billing_account_id ,
	event_name ,
	nullif(consumed_at, '')::timestamp as  consumed_at  ,
	json_extract_path_text(payload ,'state') as state,
	nullif(json_extract_path_text(payload ,'created_at'), '')::timestamp  as created_at,
	nullif(json_extract_path_text(payload ,'activated_at'), '')::timestamp  as activated_at,
	nullif(json_extract_path_text(payload ,'terminated_at'), '')::timestamp  as terminated_at,
	json_extract_path_text(payload ,'termination_reason') as termination_reason,
	json_extract_path_text(payload, 'goods') as assets,
		json_extract_path_text(payload, 'billing_terms') as billing_terms,
		json_extract_path_text(payload, 'duration_terms') as duration_terms,
	json_extract_path_text(payload ,'current_flow') as current_flow,
	json_extract_path_text(payload ,'purchase_term') as purchase_term,
	json_extract_path_text(payload ,'ending_terms') as ending_terms,
	json_extract_path_text(payload ,'action_type') as action_type,
	json_extract_path_text(payload ,'store_code') as store_code,
	NULLIF(json_extract_path_text(payload ,'new_discount'),'') AS new_discount,
	NULLIF(json_extract_path_text(payload ,'discount_removed'),'') AS discount_removed,
	json_extract_path_text(json_extract_path_text(payload ,'grover_care_terms'),'price_in_cents') AS grover_care_price_in_cents,
    	json_extract_path_text(json_extract_path_text(payload ,'grover_care_terms'),'coverage') AS grover_care_coverage,
	kafka_received_at
from s3_spectrum_kafka_topics_raw.customers_contracts s
where cast((s.year||'-'||s."month"||'-'||s."day"||' 00:00:00') as timestamp) > current_date::date-3
and consumed_at > current_date-2;
