truncate table stg_api_production.adyen_payment_requests;
insert into stg_api_production.adyen_payment_requests 
select 
replace((_airbyte_data.id::varchar),'"','')::bigint ,
replace((_airbyte_data.log::varchar),'"',''),
replace((_airbyte_data.email::varchar),'"','') ,
_airbyte_data.amount::DOUBLE PRECISION as amount,
replace((_airbyte_data.locale::varchar),'"','') ,
replace((_airbyte_data.status::varchar),'"',''),
_airbyte_data.user_id::bigint,
replace((_airbyte_data.webhook::varchar),'"',''),
replace((_airbyte_data.currency::varchar),'"','') ,
replace((_airbyte_data.order_id::varchar),'"','')::bigint ,
replace((_airbyte_data.store_id::varchar),'"','')::bigint ,
replace((_airbyte_data.meta_data::varchar),'"','') ,
replace((_airbyte_data.created_at::varchar),'"',''):: TIMESTAMP WITHOUT TIME ZONE ,
replace((_airbyte_data.deleted_at::varchar),'"',''):: TIMESTAMP WITHOUT TIME ZONE,
replace((_airbyte_data.updated_at::varchar),'"',''):: TIMESTAMP WITHOUT TIME ZONE,
replace((_airbyte_data.payment_ref::varchar),'"',''),
replace((_airbyte_data.payment_type::varchar),'"','') ,
replace((_airbyte_data.psp_reference::varchar),'"',''),
replace((_airbyte_data.refusal_reason::varchar),'"',''),
replace((_airbyte_data.contract_number::varchar),'"',''),
replace((_airbyte_data.sf_webhook_response::varchar),'"',''),
replace(json_extract_path_text(((_airbyte_data.log)::varchar),'res_body') ,'\\','') as data_log,
replace(json_extract_path_text(((_airbyte_data.log)::varchar),'req_body') ,'\\','') as customer_data_log
from stg_api_production._airbyte_raw_adyen_payment_requests;

