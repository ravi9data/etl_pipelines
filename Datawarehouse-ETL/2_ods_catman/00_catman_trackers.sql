drop table if exists skyvia.catman_trackers_subs_acquired;
create table skyvia.catman_trackers_subs_acquired as
select s.variant_sku,
		s.product_sku,
		s.start_date::date as subscription_start_date,
		1 as quantity,
		s.store_name,
		s.subscription_value as rental_plan_price,
		s.subscription_plan as rental_plan_type,
		s.subscription_name as subscription_name
from ods_production.subscription s
--left join ods_production.order_item o on s.order_item_sf_id = o.order_item_sf_id
--left join ods_production.order oo on s.order_id = oo.order_id
where subscription_start_date >= date_trunc('week',current_date-42);

drop table if exists skyvia.catman_trackers_subs_cancelled;
create table skyvia.catman_trackers_subs_cancelled as
select
	s.variant_sku,
	s.product_sku,
	s.store_name,
	s.subscription_value as rental_plan_price,
	s.cancellation_date::date,
	a.cancellation_returned_at::date as return_scanned_date,
	a.refurbishment_end_at::date as refurbishment_end_date,
	cr.cancellation_reason_new
	from
    ods_production.subscription s
    left join ods_production.subscription_cancellation_reason cr on s.subscription_id = cr.subscription_id
    left join ods_production.allocation a on a.subscription_id = s.subscription_id
	where s.cancellation_date>=CURRENT_DATE-7*7;


drop table if exists skyvia.catman_trackers_pending_allocation;
create table skyvia.catman_trackers_pending_allocation as
with a as (
			select variant_sku,
			product_sku,
			store_name,
			customer_type,
			count(*) as pending_allocations
			from
			ods_production.subscription s
			left join ods_production.customer c on s.customer_id = c.customer_id
			where status = 'ACTIVE'
			and allocation_status = 'PENDING ALLOCATION'
			and variant_sku is not null
			group by 1,2,3,4)
			, b as (
			select  variant_sku,
					product_sku,
					requested_total::int as requested_per_variant,
					assets_stock_total::int as stock_per_variant
					from
					ods_production.purchase_request
					)
			select
				a.*,
				b.requested_per_variant,
				b.stock_per_variant
				from a
				left join b on (a.variant_sku = b.variant_sku and a.product_sku = b.product_sku);


GRANT SELECT ON ALL TABLES IN SCHEMA skyvia TO  skyvia;
