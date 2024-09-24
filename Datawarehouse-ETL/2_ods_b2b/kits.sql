drop table if exists ods_b2b.kits;
create table ods_b2b.kits as 
select
	distinct
	v.kit_uuid, 
	v.order_id, 
	v.customer_id, 
	v.created_at, 
	v.store_code, 
	v.consumed_at as processed_at, 
	timestamp 'epoch' + CAST(v.published_at AS BIGINT)/1000 * interval '1 second' AS published_at,
	--order
	o.order_item_count, 
	o.store_country, 
	o.status, 
	o.is_pay_by_invoice, 
	o.paid_date 
from stg_curated.checkout_eu_cart_created_v1 v 
left join ods_production.order o
	on o.order_id = v.order_id 
where v.kit_uuid != 'null';

GRANT SELECT ON ods_b2b.kits TO tableau;
