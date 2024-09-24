drop table if exists tmp_internal_billing_payments;

create temp table tmp_internal_billing_payments as
with c as (
    select *,JSON_EXTRACT_PATH_text(payload,'uuid') as uuid,
      row_number() over (partition by uuid,event_name order by kafka_received_at asc) as idx2,
      row_number() over (partition by uuid,event_name order by kafka_received_at desc) as idx
    from stg_curated.stg_internal_billing_payments
    where is_valid_json(payload)
)
select uuid,event_name,version,payload,kafka_received_at,consumed_at,idx as idx_max
from c
where idx = 1 or idx2 = 1;   
   
   
--new infra
DROP TABLE IF EXISTS staging.asset_payments_pre_parsed;
CREATE TABLE staging.asset_payments_pre_parsed
as
with numbers as(
 	select * from public.numbers
 	where ordinal < 20),
 	asset_payment_pre as(
	select
	event_name,
	payload,
	JSON_EXTRACT_PATH_text(payload,'uuid') as uuid,
	kafka_received_at as event_timestamp,
	json_extract_path_text(payload,'line_items') as line_items,
	JSON_EXTRACT_PATH_text(payload,'type') as payment_type,
	JSON_ARRAY_LENGTH(json_extract_path_text(payload,'line_items')) as no_of_line_items,
	json_extract_array_element_text(line_items,numbers.ordinal::int,true) as line_item_split,
	json_extract_path_text(line_item_split,'contract_ids') as contract_ids,
	json_array_length(contract_ids) as no_of_contracts,
	json_extract_path_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text(payload,'line_items'),0),'order_number') as order_number,
	rank() over (partition by uuid,event_name order by kafka_received_at desc) as idx
	from
	tmp_internal_billing_payments  ap
		cross join
		numbers
	where
	numbers.ordinal < no_of_line_items
	and is_valid_json(payload)
	order by event_timestamp desc
	),
	aa as (
	select ap.* from asset_payment_pre ap
	left join stg_external_apis.discarded_payment_groups dp on ap.uuid = dp.a
	where dp.a is null )

	select
		idx,
		payload,
		event_timestamp,
		event_name,
		a.uuid,
		json_extract_array_element_text(contract_ids,numbers.ordinal::int,true) as contract_id,
		JSON_EXTRACT_PATH_text(payload,'type') as payment_type,
		JSON_EXTRACT_PATH_text(payload,'contract_type') as contract_type,
		JSON_EXTRACT_PATH_text(payload,'entity_type') as entity_type,
		JSON_ARRAY_LENGTH(json_extract_path_text(payload,'line_items'),true) as total_order_items,
		line_item_split as line_items,
		json_extract_path_text(line_item_split,'order_number') as order_number,
		JSON_EXTRACT_PATH_text(json_extract_path_text(line_item_split,'price'), 'in_cents')/100 as price,
		JSON_EXTRACT_PATH_text(json_extract_path_text(line_item_split,'price'), 'currency') as price_currency,
		case
			when JSON_EXTRACT_PATH_text(json_extract_path_text(line_item_split,'total'), 'in_cents') != ''
			then JSON_EXTRACT_PATH_text(json_extract_path_text(line_item_split,'total'), 'in_cents')/100
			else 0
		end as total_value,
		JSON_EXTRACT_PATH_text(json_extract_path_text(line_item_split,'total'), 'currency') as total_price_currency,
		JSON_EXTRACT_PATH_text(json_extract_path_text(line_item_split,'discount'), 'in_cents')/100 as discount_value,
		JSON_EXTRACT_PATH_text(json_extract_path_text(line_item_split,'discount'), 'currency') as discount_price_currency,
		json_extract_path_text(line_item_split,'quantity') as quantity,
		JSON_EXTRACT_PATH_text(json_extract_path_text(line_item_split,'base_price'), 'in_cents')/100 as base_price,
		JSON_EXTRACT_PATH_text(json_extract_path_text(line_item_split,'base_price'), 'currency') as base_price_currency,
		json_extract_path_text(line_item_split,'product_name') as product_name,
		JSON_EXTRACT_PATH_text(payload,'user_id') as user_id,
		JSON_EXTRACT_PATH_text(payload,'status') as status,
		JSON_EXTRACT_PATH_text(payload,'due_date')::timestamp as due_date,
		JSON_EXTRACT_PATH_text(payload,'consolidated') as consolidated,
		json_extract_path_text(JSON_EXTRACT_PATH_text(payload,'payment_method'),'type') as payment_method_type,
		json_extract_path_text(json_extract_path_text(JSON_EXTRACT_PATH_text(payload,'payment_method'),'data'), 'currency') as payment_method_currency,
		json_extract_path_text(json_extract_path_text(JSON_EXTRACT_PATH_text(payload,'payment_method'),'data'), 'gateway') as payment_method_gateway,
		json_extract_path_text(json_extract_path_text(JSON_EXTRACT_PATH_text(payload,'payment_method'),'data'), 'reference_id') as payment_method_reference_id,
		json_extract_path_text(json_extract_path_text(JSON_EXTRACT_PATH_text(payload,'payment_method'),'data'), 'transaction_id') as payment_method_transaction_id,
		json_extract_path_text(json_extract_path_text(JSON_EXTRACT_PATH_text(payload,'payment_method'),'data'), 'transaction_token') as payment_method_transaction_token,
		json_extract_path_text(json_extract_path_text(JSON_EXTRACT_PATH_text(payload,'payment_method'),'data'), 'parent_account_uuid') as parent_account_uuid,
		json_extract_path_text(json_extract_path_text(JSON_EXTRACT_PATH_text(payload,'payment_method'),'data'), 'account_uuid') as account_uuid,
		JSON_EXTRACT_PATH_text(payload,'billing_account_id') as billing_account_id,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'amount_due'), 'in_cents')/100 as amount_due_value,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'amount_due'), 'currency') as amount_due_currency,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_cost'), 'in_cents')/100 as shipping_cost_price,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_cost'), 'currency') as shipping_cost_currency,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'net_shipping_cost'), 'in_cents')/100 as net_shipping_cost,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'net_shipping_cost'), 'currency') as net_shipping_currency,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'tax'), 'in_cents')/100 as tax_value,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'tax'), 'currency') as tax_currency,
		json_extract_path_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text(payload,'orders'),0),'tax_rate')::decimal(10,4) as order_tax_rate,
		JSON_EXTRACT_PATH_text(payload,'country_code') as country_name,
		JSON_EXTRACT_PATH_text(payload,'payment_failed_reason') as payment_failed_reason,
		JSON_EXTRACT_PATH_text(payload,'transaction_id') as transaction_id,
		JSON_EXTRACT_PATH_text(payload,'transaction_reference_id') as transaction_reference_id
from aa a
	cross join
	numbers
	where numbers.ordinal < no_of_contracts
		and payment_type= 'purchase';
 
 
DROP TABLE IF EXISTS staging.asset_payments_new_infra;
CREATE TABLE staging.asset_payments_new_infra
as
with asset_payment_excl as (
	select
		*
	from
		staging.asset_payments_pre_parsed
	where
		((event_name not in ('refund','partial refund')))
	),
	asset_payment_idx as (
	select *,
    case
		when tax_currency = 'USD' then round((order_tax_rate * price),2)
	 else round(((order_tax_rate * price)/(1+order_tax_rate)),2)
	 end as vat_amount_fix,
	--((order_tax_rate * price)/(1+order_tax_rate))::decimal(10,4) as vat_amount_fix,
	dense_rank() over (partition by uuid order by event_timestamp desc) as idx_payment
	from asset_payment_excl
	)
	
	select
		uuid,
		max(case when event_name = 'new' then event_timestamp end ) as created_at,
		max(case when event_name in ('paid','manual_booking_summary') then event_timestamp end ) as paid_date,
		max(case when event_name = 'failed' then event_timestamp end ) as failed_date,
		max(case when event_name = 'cancelled' then event_timestamp end ) as cancellation_date,
		max(case when event_name = 'pending' then event_timestamp end ) as pending_date,
		max(event_timestamp) as updated_at
	from asset_payment_idx
	group by 1;
