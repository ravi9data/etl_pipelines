create or replace view dm_bd.v_mm_saturn_report as
with stores_ as (
select 
	distinct 
	datum as fact_date, 
	s.id, 
	s.store_type,
	s.store_number,
	s.store_name, 
	s.store_label, 
	s.country_name,
	sc.storenumber as store_code,
	sc.reportingstorecity as store_city,
	sc.reportingstoreregion as store_region
from public.dim_dates dd 
inner join ods_production.store s 
	on datum >= s.created_date::date  
left join public.partner_store_codes sc
	on s.id = sc.backofficestoreid 
where datum between '2022-09-01' and current_date
and s.account_name IN ('Media Markt', 'Saturn')
  and s.country_name IN ('Germany', 'Austria', 'Spain')
)
,subscriptions as (
select distinct
	ss.fact_date, 
	ss.store_type, 
	ss.store_label,
	ss.id as store_id,
	ss.country_name,
	ss.store_name,
	ss.store_code,
	ss.store_city,
	ss.store_region,
	s.start_date, 
	s.subscription_id, 
	s.status, 
	s.cancellation_reason_new as cancellation_reason,
	s.allocation_status, 
	ean,
	s.variant_sku, 
	s.product_sku, 
	s.product_name,
	s.category_name,
	s.subcategory_name,
	s.brand,
	s.rental_period,
	s.effective_duration,
	s.subscription_value,
	(s.committed_sub_value + s.additional_committed_sub_value) as committed_sub_value, 
	o.voucher_code,
	--date calculation
	datediff('day', s.start_date::timestamp, a.allocated_at::timestamp) as days_until_allocation,
	datediff('day', s.start_date::timestamp, s.first_asset_delivery_date::timestamp) as days_until_delivery,
	datediff('day', s.first_asset_delivery_date::timestamp, cancellation_date::timestamp) as days_until_cancel,
	datediff('day', s.first_asset_delivery_date::timestamp, last_return_shipment_at::timestamp) as days_until_return,
	datediff('day', s.first_asset_delivery_date::timestamp, current_date::timestamp) as days_since_delivery,
	dateadd('day',(case when s.store_type ='online' then 14 else 2 end),first_asset_delivery_date::timestamp) as completed_date,
	--dates
	s.first_asset_delivery_date,
	s.last_return_shipment_at, 
	a.widerruf_claim_date,
	case 
		when  s.store_type = 'online' and datediff('day', s.first_asset_delivery_date::timestamp, current_date::timestamp) > 14 then 'Above Widerruf Zone'
		when  s.store_type = 'offline' and datediff('day', s.first_asset_delivery_date::timestamp, current_date::timestamp) > 2 then 'Above Widerruf Zone'
		when  s.store_type is null then null
		else 'Below Widerruf Zone'
		end as dashboard_logic, 
	case 
    when s.start_date::DATE < '2023-09-01' and a.widerruf_claim_date is not null then 'Excluded'
    when s.start_date::DATE >= '2023-09-01' and s.is_widerruf = 'true' then 'Excluded'
		when s.store_type = 'online' and  datediff('day', s.first_asset_delivery_date::timestamp, last_return_shipment_at::timestamp) <= 14 then 'Excluded'
		when s.store_type = 'offline' and  datediff('day', s.first_asset_delivery_date::timestamp, last_return_shipment_at::timestamp) <= 2 then 'Excluded'
		when s.store_type = 'online' and  datediff('day', s.first_asset_delivery_date::timestamp, cancellation_date::timestamp) <= 14 then 'Excluded'
		when s.store_type = 'offline' and  datediff('day', s.first_asset_delivery_date::timestamp, cancellation_date::timestamp) <= 2 then 'Excluded'
		when s.allocation_status = 'PENDING ALLOCATION' then 'Excluded'
		when s.store_type is not null and s.first_asset_delivery_date is null then 'Excluded'
		when s.store_type is null then null
		else 'Included' end as logic_,
		--allocation/asset
		a.rank_allocations_per_subscription,
		a.allocation_id,
		case 
			when  a.delivered_at is not null then ROW_NUMBER () over (partition by a.allocation_id order by a.delivered_at asc)
		else null end 
		as delivery_idx,
		aa.supplier, 
		aa.asset_condition,
		aa.delivered_allocations, 
		aa.purchase_request_item_id, 
		pri.purchase_order_number,
		pri.purchased_date,
		--purchase_logic,
		case 
			when supplier in ('Saturn', 'Media Markt') 
				and purchase_order_number ilike 'GREU-PA%' 
				and (asset_condition = 'NEW' and  aa.delivered_allocations <= 1)
				and pri.purchased_date::date >= '2022-09-01' then 'Aftersourcing'
			when supplier in ('Saturn', 'Media Markt') 
				and purchase_order_number ilike 'GREU-PA%' 
				and (asset_condition != 'NEW' or aa.delivered_allocations > 1)
				then 'Include'
				--aftersourcing
			when supplier in ('Saturn', 'Media Markt') and s.store_type = 'online'
				and (asset_condition = 'NEW' and  aa.delivered_allocations <= 1)
				then 'Exclude' 
			when supplier in ('Saturn', 'Media Markt') and s.store_type = 'online'
				and (asset_condition != 'NEW' or  aa.delivered_allocations > 1)
				then 'Include' 
				--online
			when supplier not in ('Saturn', 'Media Markt') and s.store_type = 'online' and asset_condition = 'AGAN' then 'Include'
			when supplier not in ('Saturn', 'Media Markt') and s.store_type = 'online' and  asset_condition = 'NEW' then 'Include'
			--non mm supplier
			when  s.store_type = 'offline' then null
			when  s.store_type is null then null
			when  rank_allocations_per_subscription > 1 or rank_allocations_per_subscription is null then null
			when  dashboard_logic ='Below Widerruf Zone' or logic_ ='Excluded' then null
			--null
			else 'check'
			end as included_in_purchase_list_v1
	from stores_ ss
	left join master.subscription s 
		on s.start_date::date = ss.fact_date
		and s.store_id = ss.id 
	left join master."order" o
	    on o.order_id = s.order_id
	left join ods_production.allocation a 
		on a.subscription_id = s.subscription_id 
	left join public.partner_store_codes sc
		on s.store_id = sc.backofficestoreid 
	left join master.asset_historical aa
		on a.asset_id = aa.asset_id
		and a.rank_allocations_per_subscription = 1
		and a.delivered_at::date = aa.date
	left join ods_production.purchase_request_item pri 
		on aa.purchase_request_item_id = pri.purchase_request_item_id
	where s.account_name IN ('Media Markt', 'Saturn')
	and s.start_date::date >= '2022-09-01'
	order by 1, 5 asc)
	,aftersourcing as (
	select 
	distinct 
		pri.purchased_date, 
		pri.request_id,
		pri.purchase_order_number, 
		pri.variant_sku, 
		pri.product_sku, 
		pri.supplier_name, 
		sum(purchase_quantity) as purchased_quantity,
		sum(pri.delivered_quantity) as delivered_quantity
from ods_production.purchase_request_item pri 
where (supplier_name in ('Media Markt', 'Saturn')
	and purchased_date::date >= '2022-09-01')
	and purchase_order_number ilike 'GREU-PA%' 
and request_status !='CANCELLED'
and purchased_date is not null
group by 1, 2, 3, 4, 5, 6 
order by 1 asc)
, agg_aftersourcing as (
select 
	distinct 
		variant_sku, 
        max(purchased_date) as max_aftersourcing_purchased,
		sum(purchased_quantity) as total_purchased
from aftersourcing
group by 1)
, b as(
	select
	distinct 
		s.variant_sku, 
		a.total_purchased,
        max_aftersourcing_purchased,
		count(distinct s.subscription_id) as total_acquired
	from subscriptions s
		left join agg_aftersourcing a
			on a.variant_sku = s.variant_sku
	where logic_ ='Included' and s.store_type ='online' and dashboard_logic ='Above Widerruf Zone' and included_in_purchase_list_v1 ='Include'
		and rank_allocations_per_subscription = 1
group by 1, 2, 3
)
select
	case 
		when s.fact_date::date >= p.date then null
		when dashboard_logic ='Above Widerruf Zone' and logic_ = 'Included' and s.store_type ='online' and included_in_purchase_list_v1 = 'Include'
			and total_purchased is null and total_acquired is not null then 'Never Purchased'
		when dashboard_logic ='Above Widerruf Zone' and logic_ = 'Included' and s.store_type ='online' and included_in_purchase_list_v1 = 'Include' 
			and total_purchased < total_acquired then 'Purchased below Acquired'
		when dashboard_logic ='Above Widerruf Zone' and logic_ = 'Included' and s.store_type ='online' and included_in_purchase_list_v1 = 'Include'
			and total_purchased = total_acquired then 'No Action'
		when dashboard_logic ='Above Widerruf Zone' and logic_ = 'Included' and s.store_type ='online' and included_in_purchase_list_v1 = 'Include'
			and total_purchased > total_acquired then 'Surplus'
		when dashboard_logic !='Above Widerruf Zone'  then null
		when logic_ != 'Included' then null
		when s.store_type !='online'  then null
		when included_in_purchase_list_v1 != 'Include' then null
			else 'review'
		end as purchase_logic, 
		p.date as end_of_life_date,
        max_aftersourcing_purchased,
		case 
			when purchase_logic is not null then 
			row_number() over (partition by s.variant_sku, purchase_logic  order by s.completed_date asc)
		else null end 
		as item_idx,
		b.total_purchased,
		b.total_acquired,
		s.*
from subscriptions s
	left join b 
		on s.variant_sku = b.variant_sku
	left join staging_airbyte_bi.eol_msh_products p
		on p.sku = s.variant_sku
WITH NO SCHEMA BINDING;