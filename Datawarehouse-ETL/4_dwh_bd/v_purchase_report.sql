CREATE OR REPLACE VIEW dm_bd.v_purchase_report as
with 
total_prs_ as (
select  
	case 
		when pri.supplier_name ='BAUR Versand (GmbH & Co KG)' then 'Baur'
		when pri.supplier_name ='Conrad' then 'Conrad'
		when pri.supplier_name ='Gravis' then 'Gravis Online'
	else null end as supplier_name_,
	pri.request_id, 
	pri.variant_sku, 
	pri.product_sku, 
	pri.purchase_order_number, 
	purchased_date::date,
	pri.purchase_quantity,
	pri.delivered_quantity,
	row_number() over(partition by variant_sku, supplier_name_ order by purchased_date asc) as item_idx
from ods_production.purchase_request_item pri 
where (supplier_name ='BAUR Versand (GmbH & Co KG)'
	or (supplier_name ='Gravis' and purchased_date::date >= '2022-05-24')
	or (supplier_name= 'Conrad' and purchased_date::date >= '2022-05-25'))
		and request_status !='CANCELLED'
		and purchased_date is not null
		and (request_id not in ('PR-00018053','PR-00018216','PR-00018219','PR-00018264','PR-00019090')
		and pri.purchase_order_number ilike 'GREU-PA%' or (supplier_name ='Gravis' and pri.purchase_order_number not ilike 'GREU-PO%'))
			order by purchased_date asc
)
,
purchase_info as (
select distinct 
	pri.variant_sku, 
	pri.product_sku, 
	pri.supplier_name_,
	min(purchased_date)::date as min_date, 
	max(purchased_date)::date as max_date, 
	sum(purchase_quantity) as purchased_quantity,
	sum(delivered_quantity) as delivered_quantity
from total_prs_ pri
	group by 1, 2, 3 order by 4 desc )
, subscriptions_ as (
select distinct 
	s.subscription_id, 
	s.status, 
	s.variant_sku, 
	v.ean,
	s.product_sku, 
	s.product_name, 
	s.category_name,
	s.subcategory_name, 
	s.store_name,
	s.store_label,
	s.start_date, 
	s.cancellation_date,
	s.subscription_value,
	(s.committed_sub_value + s.additional_committed_sub_value) as committed_sub_value, 
	row_number() over(partition by a.subscription_id order by a.allocated_at desc) as idx,
	datediff('day', first_asset_delivery_date, cancellation_date) as days_until_cancel,
	datediff('day', s.first_asset_delivery_date, last_return_shipment_at) as days_until_return,
	datediff('day', s.first_asset_delivery_date, current_date) as days_since_delivery,
	dateadd('day',14,first_asset_delivery_date)	as completed_date,
	dateadd('day',44,first_asset_delivery_date)	as commission_date,
	bal.logic as baur_logic, 
	s.first_asset_delivery_date,
	s.last_return_shipment_at, 
	a.widerruf_claim_date,
	case 
		when widerruf_claim_date is not null then 'Excluded'
		when datediff('day', s.first_asset_delivery_date, last_return_shipment_at) < 14 then 'Excluded'
		else 'Included' end as logic_,
	case 
		when s.store_name = 'Gravis Online' and days_since_delivery >= 44 then 'Commission'
		when s.store_name = 'Gravis Online' and days_since_delivery < 44 then 'Purchase'
		when s.store_name = 'Conrad' and days_since_delivery >= 44 then 'Commission'
		when s.store_name = 'Conrad' and days_since_delivery < 44 then 'Purchase'
		when s.store_name ='Baur' and bal.logic = 'Purchase' and days_since_delivery < 44  then 'Purchase'
		when s.store_name ='Baur' and bal.logic = 'Purchase' and days_since_delivery >= 44 then 'Commission'
		when s.store_name ='Baur' and bal.logic = 'No purchase' then 'Commission'
		when s.store_name ='Baur' and bal.logic is null then 'No Logic'
		else 'check'
	end as purchase_logic
	from master.subscription s 
left join ods_production.allocation a 
	on a.subscription_id = s.subscription_id 
left join ods_production.variant v 
	on v.variant_sku = s.variant_sku 
left join stg_external_apis.baur_asset_list bal  
		on s.variant_sku = bal.variant_sku 
		and s.store_name = 'Baur'
where 
	(s.store_name ='Baur'
	or (s.store_name ='Gravis Online' and s.start_date::date >= '2022-05-24')
	or (s.store_name = 'Conrad' and s.start_date::date >= '2022-05-25'))
	and s.first_asset_delivery_date is not null
	and datediff('day', s.first_asset_delivery_date, current_date) > 14
	)
	, b as(
	select
	distinct 
		s.variant_sku, 
		s.store_label, 
 		p.purchased_quantity as total_purchased,
		count(distinct s.subscription_id) as total_acquired
	from subscriptions_ s
	left join purchase_info p
		on p.variant_sku = s.variant_sku and p.supplier_name_ = s.store_name
	where idx = 1 and logic_ = 'Included'
	and completed_date::date <=current_date
group by 1, 2, 3)
,k as (
select 
case 
	when (baur_logic != 'No purchase' or baur_logic is null)  and total_purchased is null and total_acquired is not null then 'Never Purchased'
	when (baur_logic != 'No purchase' or baur_logic is null) and total_purchased < total_acquired then 'Purchased below Acquired'
	when (baur_logic != 'No purchase' or baur_logic is null)  and total_purchased = total_acquired then 'No Action'
	when (baur_logic != 'No purchase' or baur_logic is null) and total_purchased > total_acquired then 'Surplus'
	when baur_logic = 'No purchase' then 'Only Commission'
	end as logic_for_variants, 
	row_number() over(partition by s.variant_sku, s.store_label order by s.completed_date asc) as item_idx,
	b.total_purchased,
	b.total_acquired,
	s.*
from subscriptions_ s
inner join b
	on b.store_label = s.store_label and s.variant_sku = b.variant_sku
where idx = 1 
)
select 
*,
	case 
		when purchase_logic = 'Commission' and store_name = 'Baur' then '0.10'
		when purchase_logic = 'Commission' and store_name = 'Gravis Online' then '40.00'
		when purchase_logic = 'Commission' and store_name = 'Conrad' then '0.05'
		end as commission_value,
	CASE 
		when purchase_logic = 'Commission' and store_name = 'Baur' then 'Percentage'
		when purchase_logic = 'Commission' and store_name = 'Conrad' then 'Percentage'
		when purchase_logic = 'Commission' and store_name = 'Gravis Online' then 'Amount'
		end as commission_logic
from k
WITH NO SCHEMA BINDING;