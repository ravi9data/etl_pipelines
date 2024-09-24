create temp table stg_internal_billing_payments_v2 
as 
select s.*, kafka_received_at as event_timestamp 
from stg_curated.stg_internal_billing_payments s
where  is_valid_json(payload);

drop table if exists ods_production.billing_payments_final;
create table ods_production.billing_payments_final as
with billing_payments_grouping as
	(
	select
	distinct *
	from stg_internal_billing_payments_v2
	)
	,numbers as
	(
  	select
	*
	from public.numbers
  	where ordinal < 20
	)
	,order_payments as
	(
	select
		*
		,JSON_EXTRACT_PATH_text(payload,'contract_type') as rental_mode
		,JSON_EXTRACT_PATH_text(payload,'line_items') as line_item_payload
		,JSON_ARRAY_LENGTH(json_extract_path_text(payload,'line_items'),true) as total_order_items
		,json_extract_path_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text(payload,'line_items'),0),'order_number') as order_number
		,json_extract_path_text(json_extract_path_text(payload,'payment_method'),'type') as payment_method_type
		,count(*) over (partition by order_number) as total_events
		,row_number() over (partition by order_number order by event_timestamp asc) as rank_event
		,case when total_events = rank_event then true else false end as is_last_event
		,first_value(event_timestamp) over (partition by order_number order by event_timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_charge_date
	from billing_payments_grouping
--	where rental_mode = 'FLEX'
	)
	,dates as
	(
	select
		order_number,
		min(case when event_name = 'paid' then event_timestamp end ) as paid_date,
		max(case when event_name = 'failed' then event_timestamp end ) as failed_date,
		max(case when event_name = 'refund' then event_timestamp end ) as refund_date
	from order_payments
	group by 1
	)
	,line_items_extract as
	(
	select
		op.*,
        numbers.ordinal as line_item_no,
		json_extract_array_element_text(line_item_payload,numbers.ordinal::int, true) as line_item
	from order_payments op
	cross join numbers
	where numbers.ordinal < total_order_items
	)
	,order_prices as
	(
	select
		*,
		JSON_EXTRACT_PATH_text(line_item,'quantity') as line_item_quantity,
		nullif((JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_item,'base_price'),'in_cents')),'')/100.0 as base_price,
		nullif((JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_item,'discount'),'in_cents')),'')/100.0 as discount,
		nullif((JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_item,'price'),'in_cents')),'')/100.0 as price,
		nullif((JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_item,'total'),'in_cents')),'')/100.0  as total_price
	from line_items_extract
	)
	,paid_value as
	(
	select
		order_number,
		event_name,
		is_last_event,
		sum(line_item_quantity) as line_item_quantity_total,
		sum(base_price)as base_price,
		sum(discount) as discount,
		sum(price) as price,
		sum(total_price) as total_price
		from order_prices
	where is_last_event
	group by 1,2,3
	)
	select
		op.order_number,
		op.event_name,
		payment_method_type,
		total_order_items,
		rental_mode,
		line_item_payload,
		line_item_quantity_total,
		base_price,
		discount,
		price,
		total_price,
		da.paid_date  as paid_date,
		first_charge_date,
		da.failed_date as failed_date
	from order_payments op
	left join paid_value pv
			on op.order_number = pv.order_number and op.event_name = pv.event_name
	left join dates da
		on op.order_number = da.order_number
	where op.is_last_event;

drop table if exists stg_internal_billing_payments_v2;
