delete from staging.grover_card_reservations
where consumed_at >= current_date -2;

insert into staging.grover_card_reservations
select
event_name,
consumed_at ,
json_extract_path_text(payload,'id') AS id,
json_extract_path_text(payload,'amount') AS amount,
json_extract_path_text(payload,'merchant_name') AS merchant_name,
json_extract_path_text(payload,'user_id') AS user_id,
json_extract_path_text(payload,'date') AS date,
case when json_extract_path_text(payload,'transaction_id') = '' then null else
json_extract_path_text(payload,'transaction_id') end AS transaction_id,
case when json_extract_path_text(payload,'return_transaction_id') = '' then null
else json_extract_path_text(payload,'return_transaction_id') end AS return_transaction_id,
json_extract_path_text(payload,'store') AS store,
case when json_extract_path_text(payload,'trace_id') ='' then null 
else json_extract_path_text(payload,'trace_id') end AS trace_id,
json_extract_path_text(payload,'foeign_payment') AS foeign_payment,
NULLIF(json_extract_path_text(payload,'reason'),' ') AS reason,
case when json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'card_id') = '' then null
else json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'card_id') end as card_id,
case when json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'merchant'),'id') = '' then null
else json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'merchant'),'id') end as merchant_id,
case when json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'merchant'),'country_code') = '' then null
else json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'merchant'),'country_code') end as merchant_country_code,
case when json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'merchant'),'category_code') = '' then null
else json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'merchant'),'category_code') end as merchant_category_code,
case when json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'merchant'),'town') = '' then null
else json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'merchant'),'town') end as merchant_town,
case when json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'original_amount'),'currency') = '' then null
else json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'original_amount'),'currency') end as merchant_currency,
case when json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'original_amount'),'fx_rate') = '' then null
else json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'original_amount'),'fx_rate') end as merchant_fx_rate,
case when json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'original_amount'),'fx_markup') = '' then null
else json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'original_amount'),'fx_markup') end as merchant_fx_markup,
case when json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'original_amount'),'issuer_fee') = '' then null
else json_extract_path_text(json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'original_amount'),'issuer_fee') end as merchant_issuer_fee,
case when json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'pos_entry_mode') = '' then null
else json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'pos_entry_mode') end as pos_entry_mode,
case when json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'transaction_date') = '' then null
else json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'transaction_date') end as transaction_date,
case when json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'transaction_time') = '' then null
else json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'transaction_time') end as transaction_time,
case when json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'auth_code') = '' then null
else json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'auth_code') end as auth_code,
case when json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'acquirer_id') = '' then null
else json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'acquirer_id') end as acquirer_id,
case when json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'terminal_id') = '' then null
else json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'terminal_id') end as terminal_id,
case when json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'transaction_type') = '' then null
else json_extract_path_text(json_extract_path_text(payload, 'meta_info'),'transaction_type') end as transaction_type
FROM
s3_spectrum_kafka_topics_raw.grover_card_reservations s
where cast((s.year||'-'||s."month"||'-'||s."day"||' 00:00:00') as timestamp) > current_date::date-3
AND consumed_at >= current_date -2;