create or replace view dm_operations.v_bf_pending_allocations_current AS
	with paid_res as (
	select 
		ir.uid,
		ir.sku_variant_code ,
		ir.order_number ,
		v.variant_name ,
		case when ir.store_id in (14, 629, 621) then 'US' else 'EU' end as region,
		ir.store_id ,
		s.store_short as store_name,
		iva.availability_mode ,
		s.country_name,
		ir.customer_type ,
		ir.order_mode ,
		ir.initial_quantity ,
		ir.quantity::int ,
		convert_timezone('CET', ir.paid_at) as paid_at
	from ods_production.inventory_reservation ir 
	left join ods_production.store s 
	on ir.store_id = s.id 
	left join ods_production.variant v 
	on ir.sku_variant_code = v.variant_sku
	left join ods_production.inventory_store_variant_availability iva 
	on (ir.store_id = iva.store_id and ir.sku_variant_code = iva.sku_variant_code)
	--
	-- for one res. status is not correct.
	-- so let's go with timestamps
	where ir.paid_at is not null -- ='paid'
	and ir.deleted_at is null 
	and ir.fulfilled_at is null
	and ir.expired_at  is null
	and ir.declined_at is null 
	and ir.cancelled_at  is null)
, contracts as (
	select distinct
		nullif(JSON_EXTRACT_PATH_text(payload,'order_number'),'') as order_number ,
		nullif(JSON_EXTRACT_PATH_text(payload,'id'),'')as contract_id,
		nullif(JSON_EXTRACT_PATH_text((json_extract_array_element_text(nullif(JSON_EXTRACT_PATH_text(payload,'goods'),''),0)),'variant_sku'),'')as variant_sku,
		nullif(case when event_name = 'extended' 
					then json_extract_path_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'new'), 'price')
					else JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'price') end, '')::float as amount,
		nullif(case when event_name = 'extended' 
					then json_extract_path_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'new'), 'currency')
					else JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'currency') end,'') as currency
	from stg_kafka_events_full.stream_customers_contracts_v2
	where order_number in (select order_number from paid_res where order_mode='flex')
	union all 
	select 
		s.spree_order_number__c as order_number , 
		s.subscription_id__c as contract_id , 
		o.f_product_sku_variant__c as variant_sku , 
		s.amount__c  as amount, 
		s.currency__c as currency
	from stg_salesforce.subscription__c s 
	left join stg_salesforce.orderitem o 
	on s.order_product__c = o.id 
	where spree_order_number__c in (select order_number from paid_res where order_mode='flex_legacy')
	)
select 
	pr.* ,
	c.contract_id,
	c.amount,
	c.currency
from paid_res pr 
left join contracts c 
on pr.order_number = c.order_number and pr.sku_variant_code = c.variant_sku
with no schema binding;