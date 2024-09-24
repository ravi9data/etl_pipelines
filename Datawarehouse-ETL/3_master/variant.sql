BEGIN;

DELETE FROM master.variant;

INSERT INTO master.variant
with rental_plans as (
select product_id,store_id,listagg(minimum_term_months, '  |  ' ) as rental_plans
,max(product_store_rank) product_rank --Although rank is always the same across same store and product but since the table has multiple rows for the same prod, store per rental plan, it is added that max, max or min doesn't mean anything now 
from ods_production.rental_plans 
where store_id = 1--it is agreed with shikhar at the moment to use germany as the rank, but in the future it will change
group by 1,2
	),
 a as(
	select variant_sku,
	week_date,
	price,
	LAST_VALUE(week_date) over (partition by variant_sku order by week_date ASC rows BETWEEN  UNBOUNDED PRECEDING AND  UNBOUNDED FOLLOWING ) as last_week_date,
	Case when last_week_date = week_date then true else false end as is_last_week
	 from ods_external.mm_price_data),
last_mm_price as (
	 select variant_sku, price as last_mm_price
	 from a where is_last_week is true
	),
subs as (
    select s.variant_sku,
    avg(s.minimum_term_months) as avg_rental_duration,
    count(distinct s.subscription_id) as acquired_subscriptions,
    count(case when (datediff('d',s.start_date,CURRENT_DATE)<=90) then s.subscription_id end) as Acquired_subs_last_3months,
    count(Case when s.status = 'ACTIVE' then s.subscription_id end) as active_subs,
    sum(case when s.status = 'ACTIVE' then s.subscription_value end) as active_subs_value,
    sum (sc.net_subscription_revenue_paid) as net_revenue_paid
    from ods_production.subscription s 
    left join ods_production.subscription_cashflow sc on s.subscription_id = sc.subscription_id
    group by 1),
assets as (
	select variant_sku,
	Sum(initial_price) as total_investment,
	count(asset_id) as total_assets_qty,
	sum(case when days_since_purchase <= 90 then initial_price end) as investment_last_3months,
	count(case when days_since_purchase <= 90 then asset_id end) as assets_purchased_last_3months,
	count(case when days_since_purchase <= 90 and (supplier in ('Media Markt')) then asset_id end) as assets_purchased_last_3months_mm,
		count(case when days_since_purchase <= 90 and (supplier in ('Saturn')) then asset_id end) as assets_purchased_last_3months_saturn,
    	avg(initial_price) as avg_purchase_price,
	avg(case when days_since_purchase <= 90 then initial_price end) as avg_purchase_price_last_3months,
	count(case when asset_status_original= 'ON LOAN' THEN asset_id end) as assets_on_loan,
	count(case when asset_status_original= 'IN STOCK' THEN asset_id end) as assets_in_stock,
	count(case when asset_status_original= 'DEBT COLLECTION' THEN asset_id end) as assets_on_debt_collection
	from ods_production.asset
	group by 1),
orders as (	
	select
	variant_sku,
	count(case when o.submitted_date is not null then o.order_id end) as submitted_orders,
	count(case when o.status = 'PAID' then o.order_id end) as paid_orders,
	count(case when o.status = 'CANCELLED' then o.order_id end) as cancelled_orders,
	count(case when o.status = 'DECLINED' then o.order_id end) as declined_orders
	from ods_production.order_item oi
	inner join ods_production.order o on oi.order_id = o.order_id
	group by 1)
	Select 
	v.variant_id,
	v.variant_sku,
	v.variant_name,
	v.variant_color,
	v.variant_updated_at,
	v.availability_state,
	v.ean,
	mm.last_mm_price,
	p.product_id,
	p.product_sku,
	p.created_at as product_created,
	p.product_name,
	p.category_name,
	p.subcategory_name,
	p.brand,
	p.slug as Pdp_Url,
	p.market_price,
	coalesce(rp.product_rank::integer, p."rank") rank,--in case some rows come with null values
	rp.rental_plans,
	pr.pending_allocation_mm, 
	pr.assets_stock_mm,
	pr.assets_stock_mm_new,
	pr.assets_stock_mm_agan,
	coalesce(a.assets_purchased_last_3months_mm,0) as assets_purchased_last_3months_mm,
	pr.assets_book_mm,
	pr.requested_mm, 
	pr.approved_pending_manual_review_mm,
	pr.pending_allocation_saturn,
	pr.assets_stock_saturn,
	pr.assets_stock_saturn_new,
	pr.assets_stock_saturn_agan,
	coalesce(a.assets_purchased_last_3months_saturn,0) as assets_purchased_last_3months_saturn,
	pr.requested_saturn,
	pr.approved_pending_manual_review_saturn,
	pr.pending_allocation_conrad,
	pr.assets_stock_conrad,
	pr.requested_conrad,
	pr.approved_pending_manual_review_conrad,
	pr.pending_allocation_gravis,
	pr.assets_stock_gravis,
	pr.requested_gravis,
	pr.approved_pending_manual_review_gravis,
	pr.pending_allocation_UNITO,
	pr.assets_stock_quelle,
	pr.requested_quelle,
	pr.approved_pending_manual_review_UNITO,
	pr.pending_allocation_weltbild,
	pr.assets_stock_weltbild,
	pr.requested_weltbild,
	pr.approved_pending_manual_review_weltbild,
	pr.pending_allocation_alditalk,
	pr.assets_stock_alditalk,
	pr.requested_alditalk,
	pr.approved_pending_manual_review_alditalk,
	pr.pending_allocation_comspot,
    pr.assets_stock_comspot,
    pr.requested_comspot,
    pr.approved_pending_manual_review_comspot,
	pr.pending_allocation_shifter,
    pr.assets_stock_shifter,
    pr.requested_shifter,
    pr.approved_pending_manual_review_shifter,
	pr.pending_allocation_irobot,
    pr.assets_stock_irobot,
    pr.requested_irobot,
    pr.approved_pending_manual_review_irobot,
	pr.pending_allocation_samsung,
    pr.assets_stock_samsung,
    pr.requested_samsung,
    pr.approved_pending_manual_review_samsung,
	pr.pending_allocation_others,
	pr.assets_stock_others,
	pr.requested_others,
	pr.approved_pending_manual_review_others,
	s.avg_rental_duration,
	COALESCE(s.acquired_subscriptions,0) AS acquired_subscriptions,
    COALESCE(s.Acquired_subs_last_3months,0) AS Acquired_subs_last_3months,
    COALESCE(s.active_subs,0) AS active_subs,
    COALESCE(s.active_subs_value,0) AS active_subs_value,
    COALESCE(s.net_revenue_paid,0) AS net_revenue_paid,
    COALESCE(a.total_investment,0) as total_investment,
	COALESCE(a.total_assets_qty,0) as total_assets_qty,
	COALESCE(a.investment_last_3months,0) as investment_last_3months,
	COALESCE(a.assets_purchased_last_3months,0) as assets_purchased_last_3months,
	COALESCE(a.avg_purchase_price,0) as avg_purchase_price,
	COALESCE(a.avg_purchase_price_last_3months,0) as avg_purchase_price_last_3months,
	COALESCE(a.assets_on_loan,0) as assets_on_loan,
	COALESCE(a.assets_in_stock,0) as assets_in_stock ,
	COALESCE(a.assets_on_debt_collection,0) as assets_on_debt_collection,
	COALESCE(o.submitted_orders,0) as submitted_orders,
	COALESCE(o.paid_orders,0) as paid_orders ,
	COALESCE(o.cancelled_orders,0) as cancelled_orders,
	COALESCE(o.declined_orders,0) as declined_orders
	from ods_production.variant v 
	left join ods_production.product p 
			on v.product_id = p.product_id
	left join rental_plans rp
			on rp.product_id = p.product_id
	left join ods_production.purchase_request pr
			on pr.variant_sku = v.variant_sku
	left join subs s
			on s.variant_sku = v.variant_sku
	left join assets a 
			on a.variant_sku = v.variant_sku
	left join orders o 
			on o.variant_sku = v.variant_sku
	left join last_mm_price mm
			on mm.variant_sku = v.variant_sku
	;

COMMIT;
