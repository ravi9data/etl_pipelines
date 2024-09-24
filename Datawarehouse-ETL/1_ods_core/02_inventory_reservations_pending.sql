drop table if exists ods_production.inventory_reservation_pending ;
create table ods_production.inventory_reservation_pending as
with paid_res as (
	select 
		ir.uid as reservation_id,
		ir.sku_variant_code as variant_sku,
		p.product_sku,
		v.variant_name ,
		p.category_name,
		p.subcategory_name,
		ir.order_number as order_id,
		case when ir.store_id in (14, 629, 621) then 'US' else 'EU' end as region,
		ir.store_id ,
		s.store_short,
		iva.availability_mode ,
		s.country_name,
		ir.customer_type ,
		ir.order_mode ,
		ir.initial_quantity ,
		ir.quantity::int ,
		convert_timezone('CET', ir.paid_at) as paid_at,
		datediff('day', convert_timezone('CET', ir.paid_at) , current_date) as subscription_to_now_in_days
	from ods_production.inventory_reservation ir 
	left join ods_production.store s 
	on ir.store_id = s.id 
	left join ods_production.variant v 
	on ir.sku_variant_code = v.variant_sku
	left join ods_production.product p 
	on p.product_id = v.product_id
	left join ods_production.inventory_store_variant_availability iva 
	on (ir.store_id = iva.store_id and ir.sku_variant_code = iva.sku_variant_code)
	--
	-- some res. status is not correct.
	-- so let's go with timestamps
	where ir.paid_at < current_timestamp
	and ir.deleted_at is null 
	and ir.fulfilled_at is null
	and ir.expired_at  is null
	and ir.declined_at is null 
	and ir.cancelled_at  is null),
pri_counts as(
	select 
		v.sku_variant__c  as variant_sku ,
		count(item.id) pri_count  ,
		sum(item.effective_quantity__c - coalesce(item.delivered__c, 0)) pri_amount
	from stg_salesforce.purchase_request_item__c item
	left join stg_salesforce.purchase_request__c request 
	on request."Id" = item.purchase_request__c
	left join stg_salesforce."Product2" v 
	on item.variant__c = v.id 
	where request.status__c in ('NEW', 'IN DELIVERY')
	and v.sku_variant__c in (select variant_sku from paid_res)
	group by 1
),
assets_in_stock as (
	select
		a.f_product_sku_variant__c as variant_sku ,
		count(id) as assets_in_stock
	from stg_salesforce.asset a
	where a.status  = 'IN STOCK'
	  and a.f_product_sku_variant__c in (select variant_sku from paid_res)
	group by 1
)
select  
	pr.reservation_id,
	pr.variant_sku,
	pr.product_sku,
	pr.variant_name,
	pr.category_name,
	pr.subcategory_name,
	pr.order_id,
	coalesce (s.subscription_name, s.subscription_id ) as subscription_id ,
	pr.region,
	pr.store_id,
	pr.store_short,
	pr.availability_mode,
	pr.country_name,
	pr.customer_type,
	pr.order_mode,
	pr.initial_quantity,
	pr.quantity,
	pr.paid_at,
	pr.subscription_to_now_in_days,
	s.subscription_value as amount,
	s.currency,
	coalesce(pri.pri_count, 0) as pri_count,
	coalesce(pri.pri_amount, 0) as pri_amount,
	coalesce(ais.assets_in_stock, 0) as assets_in_stock
from paid_res pr 
left join ods_production.subscription s 
on pr.order_id = s.order_id and pr.variant_sku = s.variant_sku
left join pri_counts pri 
on pri.variant_sku = pr.variant_sku
left join assets_in_stock ais 
on ais.variant_sku = pr.variant_sku
where s.status = 'ACTIVE' and s.allocation_status = 'PENDING ALLOCATION';


drop table if exists ods_production.inventory_reservation_pending_historical ;
create table ods_production.inventory_reservation_pending_historical as
with hours as (
	select ordinal as hours 
	from public.numbers 
	where ordinal < 24
),
days as (
	select datum, hours 
	from public.dim_dates 
	cross join hours 
	where datum >= '2022-11-07' and datum <= current_date
),
ir_pre as (
	select 
		uid,
		case when ir.store_id in (14, 629, 621) then 'US' else 'EU' end as region,
		st.store_short,
		ir.customer_type,
		least(ir.fulfilled_at,
			  ir.cancelled_at,
			  ir.deleted_at,
			  ir.expired_at,
			  ir.declined_at,
			  a.allocated_at, --we may not receive event, let's check if there is an allocation
			  sb.cancellation_date --also check if subs has been cancelled
			  ) as end_date,
		ir.paid_at
	from ods_production.inventory_reservation ir 
	left join ods_production.store st 
	on ir.store_id = st.id 
	left join ods_production.subscription sb 
	on ir.order_number = sb.order_id and ir.sku_variant_code = sb.variant_sku
	left join (select subscription_id, max(allocated_at) allocated_at 
		   		from ods_production.allocation group by 1) a 
	on sb.subscription_id = a.subscription_id 
	where sb.subscription_id is not null
)
select 
	datum, 
	hours,
	region,
	store_short ,
	customer_type,
	count(uid) pending_allocations
from ir_pre, days
where convert_timezone('CET', paid_at) < dateadd('hour', hours, datum)
	  and (end_date is null or end_date	> dateadd('hour', hours, datum))
	  and dateadd('hour', hours, datum) < current_timestamp 
group by 1, 2, 3, 4, 5;


GRANT SELECT ON ods_production.inventory_reservation_pending to hightouch;
GRANT SELECT ON ods_production.inventory_reservation_pending_historical TO tableau;
GRANT SELECT ON ods_production.inventory_reservation_pending TO tableau;
