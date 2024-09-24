drop table if exists skyvia.product_data_livefeed;
create table skyvia.product_data_livefeed as
with rental_plans as (
	select
	product_id,
	store_id,
	max(product_store_rank) as product_store_rank,
	listagg(minimum_term_months, '  |  ' ) within group (order by minimum_term_months ASC)  as rental_plans,
	max(case when minimum_term_months = 1 then rental_plan_price_higher_price end) as rental_plan_price_1_month,
	max(case when minimum_term_months = 3 then rental_plan_price_higher_price end) as rental_plan_price_3_months,
	max(case when minimum_term_months = 6 then rental_plan_price_higher_price end) as rental_plan_price_6_months,
	max(case when minimum_term_months = 12 then rental_plan_price_higher_price end) as rental_plan_price_12_months,
	max(case when minimum_term_months = 18 then rental_plan_price_higher_price end) as rental_plan_price_18_months,
	max(case when minimum_term_months = 24 then rental_plan_price_higher_price end) as rental_plan_price_24_months
	from ods_production.rental_plans
	where store_id = 1
	group by 1,2)
		,
	fact_days AS (
	SELECT
	DISTINCT DATUM AS FACT_DAY
	FROM public.dim_dates
	WHERE DATUM <= CURRENT_DATE
	)
,last_week_active_sub as (
	select
	fact_day,
	variant_sku,
	COALESCE(sum(s.subscription_value),0) as active_sub_value_last_week,
	COALESCE(count(distinct s.subscription_id),0) as active_subs_last_week
	from fact_days f
	left join ods_production.subscription as s
   on f.fact_day::date >= s.start_date::date and
   F.fact_day::date < coalesce(s.cancellation_date::date, f.fact_day::date+1)
   where fact_day = DATEADD('day',-1,DATE_TRUNC('week',CURRENT_DATE))
   group by 1,2),
 subs as (
	select variant_sku,
	avg(subscription_value) as avg_sub_value,
	count(case when datediff('day',start_date::timestamp,current_date) <= 7 then subscription_id end ) as created_subs_last_week,
	count(case when datediff('day',start_date::timestamp,current_date) <= 14 then subscription_id end ) as created_subs_last_2weeks,
	count(case when datediff('day',start_date::timestamp,current_date) <= 28 then subscription_id end ) as created_subs_last_4weeks,
	sum(case when datediff('day',start_date::timestamp,current_date) <= 7 then subscription_value end ) as created_asv_last_week,
	sum(case when datediff('day',start_date::timestamp,current_date) <= 14 then subscription_value end ) as created_asv_last_2weeks,
	sum(case when datediff('day',start_date::timestamp,current_date) <= 28 then subscription_value end ) as created_asv_last_4weeks,
	count(case when datediff('day',cancellation_date::timestamp,current_date) <= 7 then subscription_id end ) as cancelled_subs_last_week,
	count(case when datediff('day',cancellation_date::timestamp,current_date) <= 14 then subscription_id end ) as cancelled_subs_last_2weeks,
	count(case when datediff('day',cancellation_date::timestamp,current_date) <= 28 then subscription_id end ) as cancelled_subs_last_4weeks,
	count(case when datediff('day',start_date::timestamp,current_date) <= 90 then subscription_id end ) as created_subs_last_3months,
	count(case when datediff('day',cancellation_date::timestamp,current_date) <= 90 then subscription_id end) as cancelled_subs_last_3months
	from ods_production.subscription
	group by 1),
orders as(
	select oi.variant_sku,
	count(case when datediff('day',paid_date::timestamp,current_date) <= 90 and o.status = 'PAID' then oi.order_id end) as paid_orders_last_3months,
	count(case when datediff('day',submitted_date::timestamp,current_date) <= 90  then oi.order_id end) as submitted_orders_last_3months
	from ods_production.order_item oi
	left join ods_production."order" o on o.order_id = oi.order_id
	left join ods_production.order_marketing_channel omc on omc.order_id = oi.order_id
	group by 1),
product_orders as (
select oi.product_sku,
	count(case when datediff('day',paid_date::timestamp,current_date) <= 28 and o.status = 'PAID' then oi.order_id end) as paid_orders_last_4weeks_per_product,
	count(case when datediff('day',submitted_date::timestamp,current_date) <= 7  and omc.is_paid is True then oi.order_id end) as submitted_orders_last_week_per_product_paid_traffic,
	count(Case when datediff('day',submitted_date::timestamp,current_date) <= 7  and omc.is_paid is False then oi.order_id end) as submitted_orders_last_week_per_product_organic_traffic,
	count(case when datediff('day',submitted_date::timestamp,current_date) <= 14  and omc.is_paid is True then oi.order_id end) as submitted_orders_last_2weeks_per_product_paid_traffic,
	count(Case when datediff('day',submitted_date::timestamp,current_date) <= 14  and omc.is_paid is False then oi.order_id end) as submitted_orders_last_2weeks_per_product_organic_traffic,
	count(case when datediff('day',submitted_date::timestamp,current_date) <= 28  and omc.is_paid is True then oi.order_id end) as submitted_orders_last_4weeks_per_product_paid_traffic,
	count(Case when datediff('day',submitted_date::timestamp,current_date) <= 28  and omc.is_paid is False then oi.order_id end) as submitted_orders_last_4weeks_per_product_organic_traffic
	from ods_production.order_item oi
	left join ods_production."order" o on o.order_id = oi.order_id
	left join ods_production.order_marketing_channel omc on omc.order_id = oi.order_id
	group by 1
	),
assets as (
	select variant_sku,
	count(case when asset_status_original = 'IN STOCK' then asset_id end) as stock_on_hand,
	count(case when asset_status_grouped in ('IN STOCK','ON RENT','REFURBISHMENT','TRANSITION') then asset_id end) as stock_on_book,
	sum(case when asset_status_grouped = 'IN STOCK' then initial_price else 0 end) as price_stock_on_hand,
	sum(case when asset_status_grouped = 'ON RENT' then initial_price else 0 end) as price_on_rent,
	case when (price_on_rent+price_stock_on_hand) != 0 then  (price_on_rent/(price_on_rent+price_stock_on_hand))::decimal(10,2) else 0 end as marketable_utilisation,
	AVG(initial_price) as avg_purchase_price,
	AVG(case when months_since_purchase <=3 then initial_price end) as avg_purchase_price_last_3months,
	AVG(case when asset_status_original = 'IN STOCK' then coalesce(days_in_stock,0) end ) as avg_days_in_stock
	from ods_production.asset
	group by 1),
	inbound as (
	select variant_sku,sum(net_quantity) as incoming_qty from ods_production.purchase_request_item
	where request_status NOT IN ('CANCELLED', 'NEW')
	group by 1)
	,last_values as(
select
	variant_sku,
	last_value(case when asset_status_grouped != 'NEVER PURCHASED' then initial_price end ignore nulls) over(partition by variant_sku order by purchased_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_initial_price,
	last_value(case when asset_status_grouped != 'NEVER PURCHASED' then purchased_date end ignore nulls) over(partition by variant_sku order by purchased_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_purchased_date
	FROM
	ods_production.asset)
,last_values_grouped as (
	select *
	from last_values
	group by 1,2,3),
	traffic as (
	select
 case when pv.page_type ='pdp' then p.product_id::CHARACTER VARYING else 'Non-pdp-pages' end as product_id,
 count(distinct case when s.is_paid and datediff('d',pv.page_view_start::date,CURRENT_DATE) <= 7 then coalesce(pv.customer_id_mapped,pv.anonymous_id) end) as paid_traffic_last_week,
 count(distinct case when s.is_paid is False and datediff('d',pv.page_view_start::date,CURRENT_DATE) <= 7 then coalesce(pv.customer_id_mapped,pv.anonymous_id) end) as organic_traffic_last_week,
 count(distinct case when s.is_paid and datediff('d',pv.page_view_start::date,CURRENT_DATE) <= 14 then coalesce(pv.customer_id_mapped,pv.anonymous_id) end) as paid_traffic_last_2weeks,
 count(distinct case when s.is_paid is False and datediff('d',pv.page_view_start::date,CURRENT_DATE) <= 14 then coalesce(pv.customer_id_mapped,pv.anonymous_id) end) as organic_traffic_last_2weeks,
 count(distinct case when s.is_paid and datediff('d',pv.page_view_start::date,CURRENT_DATE) <= 28 then coalesce(pv.customer_id_mapped,pv.anonymous_id) end) as paid_traffic_last_4weeks,
 count(distinct case when s.is_paid is False and datediff('d',pv.page_view_start::date,CURRENT_DATE) <= 28 then coalesce(pv.customer_id_mapped,pv.anonymous_id) end) as organic_traffic_last_4weeks
from traffic.page_views pv
left join traffic.sessions s
  ON pv.session_id = s.session_id
left join ods_production.product p
 on p.product_sku=pv.page_type_detail
where page_view_start::date>='2019-07-01'
  and page_type ='pdp'
group by 1),
	conversion as (
Select
	p.product_id,
	case when coalesce(organic_traffic_last_4weeks,0) != 0 then (submitted_orders_last_4weeks_per_product_organic_traffic::DOUBLE PRECISION/organic_traffic_last_4weeks::DOUBLE PRECISION) else 0 end as organic_conversion_per_product_last4weeks,
	case when coalesce(paid_traffic_last_4weeks,0) != 0 then (submitted_orders_last_4weeks_per_product_paid_traffic::DOUBLE PRECISION/paid_traffic_last_4weeks::DOUBLE PRECISION )else 0 end as paid_conversion_per_product_last4weeks,
	case when coalesce(organic_traffic_last_2weeks,0) != 0 then (submitted_orders_last_2weeks_per_product_organic_traffic::DOUBLE PRECISION/organic_traffic_last_2weeks::DOUBLE PRECISION) else 0 end as organic_conversion_per_product_last2weeks,
	case when coalesce(paid_traffic_last_2weeks,0) != 0 then (submitted_orders_last_2weeks_per_product_paid_traffic::DOUBLE PRECISION/paid_traffic_last_2weeks::DOUBLE PRECISION )else 0 end as paid_conversion_per_product_last2weeks,
	case when coalesce(organic_traffic_last_week,0) != 0 then (submitted_orders_last_week_per_product_organic_traffic::DOUBLE PRECISION/organic_traffic_last_week::DOUBLE PRECISION) else 0 end as organic_conversion_per_product_lastweek,
	case when coalesce(paid_traffic_last_week,0) != 0 then (submitted_orders_last_week_per_product_paid_traffic::DOUBLE PRECISION/paid_traffic_last_week::DOUBLE PRECISION )else 0 end as paid_conversion_per_product_lastweek
	from
	ods_production.product p
	left join traffic t on t.product_id = p.product_id
	left join product_orders po on po.product_sku = p.product_sku
)
select
v.variant_sku,
v.ean,
v.upcs,
p.product_id,
p.product_sku,
p.category_name,
p.subcategory_name,
p.brand,
p.product_name,
p."rank",
v.variant_color,
p.slug as pdp_url,
v.availability_state as Availability,
v.article_number,
rp.rental_plans,
rp.product_store_rank,
rp.rental_plan_price_1_month,
rp.rental_plan_price_3_months,
rp.rental_plan_price_6_months,
rp.rental_plan_price_12_months,
rp.rental_plan_price_18_months,
rp.rental_plan_price_24_months,
p.market_price,
a.stock_on_book,
a.avg_purchase_price,
a.avg_purchase_price_last_3months,
a.price_stock_on_hand,
a.price_on_rent,
a.marketable_utilisation,
a.stock_on_hand,
coalesce(avg_days_in_stock,0) as avg_days_in_stock,
coalesce(o.submitted_orders_last_3months,0) as submitted_orders_last_3months,
o.paid_orders_last_3months,
coalesce(s.created_asv_last_week,0) as created_asv_last_week,
coalesce(s.created_asv_last_2weeks,0) as created_asv_last_2weeks,
coalesce(s.created_asv_last_4weeks,0) as created_asv_last_4weeks,
s.created_subs_last_3months,
s.cancelled_subs_last_3months,
s.avg_sub_value,
lg.last_initial_price,
datediff('month',lg.last_purchased_date,CURRENT_DATE) as months_since_last_purchase,
case when coalesce(i.incoming_qty,0) > 0 then i.incoming_qty else 0 end as incoming_qty,
created_subs_last_week,
created_subs_last_2weeks,
created_subs_last_4weeks,
cancelled_subs_last_week,
cancelled_subs_last_2weeks,
cancelled_subs_last_4weeks,
coalesce(paid_traffic_last_week,0) as paid_traffic_last_week,
coalesce(organic_traffic_last_week,0) as organic_traffic_last_week,
coalesce(paid_traffic_last_2weeks, 0) paid_traffic_last_2weeks,
coalesce(organic_traffic_last_2weeks,0) as organic_traffic_last_2weeks,
coalesce(paid_traffic_last_4weeks,0) as paid_traffic_last_4weeks,
coalesce(organic_traffic_last_4weeks,0) as organic_traffic_last_4weeks,
case when organic_conversion_per_product_last4weeks > 1 then 1 else organic_conversion_per_product_last4weeks end as organic_conversion_per_product_last4weeks,
case when paid_conversion_per_product_last4weeks > 1 then 1 else paid_conversion_per_product_last4weeks end as paid_conversion_per_product_last4weeks,
case when organic_conversion_per_product_last2weeks > 1 then 1 else organic_conversion_per_product_last2weeks end as organic_conversion_per_product_last2weeks,
case when paid_conversion_per_product_last2weeks > 1 then 1 else paid_conversion_per_product_last2weeks end as paid_conversion_per_product_last2weeks,
case when organic_conversion_per_product_lastweek > 1 then 1 else organic_conversion_per_product_lastweek end organic_conversion_per_product_lastweek,
case when paid_conversion_per_product_lastweek > 1 then 1 else paid_conversion_per_product_lastweek end as paid_conversion_per_product_lastweek,
coalesce(active_sub_value_last_week,0) as active_sub_value_last_week ,
coalesce(active_subs_last_week,0) as active_subs_last_week
from
ods_production.variant v
left join ods_production.product p on v.product_id = p.product_id
left join subs s on s.variant_sku = v.variant_sku
left join orders o on o.variant_sku = v.variant_sku
left join assets a on a.variant_sku = v.variant_sku
left join rental_plans rp on rp.product_id = p.product_id
left join inbound i on i.variant_sku = v.variant_sku
left join last_values_grouped lg on lg.variant_sku = v.variant_sku
left join traffic t on v.product_id = t.product_id
left join conversion c on c.product_id = v.product_id
left join last_week_active_sub lw on lw.variant_sku = v.variant_sku;


GRANT SELECT ON ALL TABLES IN SCHEMA skyvia TO skyvia;


drop table if exists dm_product.product_data_livefeed;
create table dm_product.product_data_livefeed
	as select * from skyvia.product_data_livefeed;

GRANT SELECT ON dm_product.product_data_livefeed TO  GROUP commercial, GROUP pricing, GROUP recommerce;
GRANT SELECT ON skyvia.product_data_livefeed TO  hightouch_pricing, hightouch, redash_pricing;
GRANT SELECT ON skyvia.product_data_livefeed TO tableau;
