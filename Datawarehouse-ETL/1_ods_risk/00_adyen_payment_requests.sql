drop table if exists trans_dev.adyen_log_json_corrected;
create table trans_dev.adyen_log_json_corrected as  
select 
id,
user_id as customer_Id,
payment_ref,
payment_type,
currency,
status,
created_at,
amount,
contract_number,
"log" as log_raw,
refusal_reason,
psp_reference,
data_log,
customer_data_log
from stg_api_production.adyen_payment_requests
where f_json_ok(replace(data_log ,'\\',''))
;

drop table if exists ods_data_sensitive.adyen_payment_requests;
create table ods_data_sensitive.adyen_payment_requests as 
select 
a.*,
json_extract_path_text(customer_data_log,'reference') as merchant_reference,
json_extract_path_text(data_log,'additionalData','refusalReasonRaw') as refusal_reason_raw,
json_extract_path_text(data_log,'additionalData','cardBin') as card_bin,
json_extract_path_text(data_log,'additionalData','paymentMethod') as card_type,
json_extract_path_text(data_log,'additionalData','cardIssuingCountry') as card_Issuing_Country,
json_extract_path_text(data_log,'additionalData','cardIssuingBank') as card_Issuing_Bank,
json_extract_path_text(data_log,'additionalData','fundingSource') as funding_source,
json_extract_path_text(data_log,'additionalData','cardSummary') as card_number,
json_extract_path_text(data_log,'fraudResult','accountScore') as fraud_score,
json_extract_path_text(data_log,'additionalData','cardHolderName') as cardholder_name,
json_extract_path_text(data_log,'resultCode') as status_,
json_extract_path_text(customer_data_log,'shopperEmail') as shopper_email,
json_extract_path_text(customer_data_log,'shopperInteraction') as shopper_interaction
from trans_dev.adyen_log_json_corrected a  
;

drop table if exists trans_dev.adyen_log_json_corrected;