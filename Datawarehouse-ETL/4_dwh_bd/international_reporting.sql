drop table if exists dwh.international_reporting;
create table dwh.international_reporting as 

--Sets the date and variants necessary for other joins 
with fact_days as(
	SELECT
		DISTINCT DATUM AS reporting_date, 
		id as store_id, 
		store_name,
		store_label,
		variant_sku,
		product_sku,
		ean,
		product_name,
		variant_color,
		brand,
		category_name, 
		subcategory_name
	FROM public.dim_dates, (select id, store_name, store_label, created_date::date from ods_production.store where  store_short = 'Grover International')
	,(select variant_sku, product_sku, variant_color, ean, product_name, category_name, subcategory_name, brand, created_at::date as created_at from ods_production.variant v 
							left join ods_production.product p on v.product_id = p.product_id)
		 where DATUM <= CURRENT_DATE and datum >= created_at::date and datum >=created_date::date)
--Impressions per product_sku (soon it will be changed to variant_sku)
,impressions as(
	SELECT
		 i.reporting_date::date as reporting_date,
		i.store_id,
		product_sku,
    	sum(impressions) as total_impressions,
    	sum(case when button_state = 'available' then impressions end) as available_impressions,
    	sum(case when button_state = 'unavailable' then impressions end) as unavailable_impressions,
    	sum(case when button_state = 'widget' then impressions end) as widget_impressions
	join ods_production.store s 
	on s.id =i.store_id 
	where store_short = 'Grover International'
	group by 1,2,3 order by 1 desc)		 
--the Landing page traffic shows how many times the customer clicked on the Grover Partner Button and landed on Grover page
,traffic as (
	select 
		pv.page_view_date::date as reporting_date,
		pv.store_id,  
		marketing_campaign as variant_sku,
		count(distinct page_view_id) as pageviews,
		count(distinct session_id) as pageview_unique_sessions,
		count(distinct anonymous_id) as users
	FROM traffic.page_views pv
	left join ods_production.store s
	on pv.store_id = s.id 
		where page_type ='landing' 
		and variant_sku is not null
		and  s.store_short = 'Grover International'
		group by 1,2,3 order by 1 desc )
--Counts the Add to cart orders
,orders AS (
	SELECT 
		o.created_date::date as reporting_date, 
		o.store_id,
		oi.variant_sku, 
		count (distinct o.order_id) AS add_to_cart,
   		count(distinct case when submitted_date is not null then o.order_id end) AS submitted_orders,
    	count(distinct case when paid_date is not null then o.order_id end) AS paid_orders
	FROM ods_production.ORDER o
		LEFT JOIN ods_production.order_item oi
		ON oi.order_id = o.order_id
		group by 1,2,3 order by 1 desc 
)
--Sum and Count acquired subscription 
,subscriptions_ as (
	select 
		s.start_date::date as reporting_date,
		s.store_id,
		s.variant_sku,
		count (distinct subscription_id) as acquired_subscription,
		sum(s.subscription_value) as acquired_subscription_value
	from ods_production.subscription s
		group by 1,2,3 order by 4 desc )
-- Information on Active Subscriptions
,active_subs_ as (
	select 
		f.reporting_date,
		f.store_id, 
		f.variant_sku, 
		sum(ss.subscription_value_eur) as active_subscription_value,
		count (distinct ss.subscription_id) as active_subscriptions,
		sum(case when ss.rental_period = 1 then 1 else 0 end) as rental_1m,
		sum(case when ss.rental_period = 3 then 1 else 0 end) as rental_3m,
		sum(case when ss.rental_period = 6 then 1 else 0 end) as rental_6m,
		sum(case when ss.rental_period = 12 then 1 else 0 end) as rental_12m,
		sum(case when ss.rental_period = 18 then 1 else 0 end) as rental_18m,
		sum(case when ss.rental_period = 24 then 1 else 0 end) as rental_24m,
		sum(case when ss.payment_method ='paypal-gateway' then 1 else 0 end) as method_paypal_gateway,
		sum(case when ss.payment_method ='PayPal' then 1 else 0 end) as method_paypal,
		sum(case when ss.payment_method ='ManualTransfer' then 1 else 0 end) as method_manual_transfer,
		sum(case when ss.payment_method ='AdyenContract' then 1 else 0 end) as method_adyen,
		sum(case when ss.payment_method ='CreditCard' then 1 else 0 end) as method_credit_card,
		sum(case when ss.payment_method ='dalenys-bankcard-gateway' then 1 else 0 end) as method_dalenys,
		sum(case when ss.payment_method ='sepa-gateway' then 1 else 0 end) as method_sepa
	from fact_days f
	left join ods_production.subscription_phase_mapping ss
	on f.reporting_date::date >= ss.fact_day::date 
       and f.reporting_date::date <= coalesce(ss.end_date::date, f.reporting_date::date+1) 
       and f.store_id = ss.store_id and f.variant_sku = ss.variant_sku
group by 1,2,3 order by 1 desc)
--Provides the total Asset investment on products from the chosen store
,asset_investment as (
	select 
		a.purchased_date::date as reporting_date, 
		s.id as store_id,
		a.variant_sku,
		sum(initial_price) as asset_investment_value,
		count (distinct asset_id) as asset_investment_count
	from ods_production.asset a 
	left join ods_production.store s 
	on s.store_name like '%' + a.supplier_account + '%'
		where supplier_account not in ('N/A') and s.store_short = 'Grover International'
		group by 1,2,3 order by 1 desc )
--JOIN
,a as(		select 
		f.reporting_date,
		f.store_id, 
		f.store_name, 
		f.store_label,
		f.variant_sku,
		f.product_sku, 
		f.ean,
		f.product_name, 
		f.variant_color,
		f.brand,
		f.category_name, 
		f.subcategory_name, 
	--impressions
		COALESCE(i.total_impressions,'0') as total_impressions,
		COALESCE(i.available_impressions,'0') as available_impressions,
		COALESCE(i.unavailable_impressions,'0') as unavailable_impressions,
		COALESCE(i.widget_impressions,'0') as widget_impressions,
	--traffic
		COALESCE(t.pageviews,'0') as page_views,
		COALESCE(t.pageview_unique_sessions,'0') as sessions,
		COALESCE(t.users,'0') as users,
	--orders
		COALESCE(os.add_to_cart,'0') as add_to_cart,
		COALESCE(os.submitted_orders,'0') as submitted_orders,
		COALESCE(os.paid_orders,'0') as paid_orders,
	--acquired subscriptions
		COALESCE(s.acquired_subscription,'0') as acquired_subscription,
		COALESCE(s.acquired_subscription_value,'0') as acquired_subscription_value,
	--active subscriptions
		COALESCE(s2.active_subscriptions, '0') as active_subscriptions,
		COALESCE(s2.active_subscription_value,'0') as active_subscription_value,
		coalesce(s2.rental_1m, '0') as rental_1m,
		coalesce(s2.rental_3m, '0') as rental_3m,
		coalesce(s2.rental_6m, '0') as rental_6m,
		coalesce(s2.rental_12m, '0') as rental_12m,
		coalesce(s2.rental_18m, '0') as rental_18m,
		coalesce(s2.rental_24m, '0') as rental_24m,
		COALESCE(s2.method_paypal_gateway, '0') as method_paypal_gateway,
		COALESCE(s2.method_paypal, '0') as method_paypal,
		COALESCE(s2.method_manual_transfer, '0') as method_manual_transfer,
		COALESCE(s2.method_adyen, '0') as method_adyen,
		COALESCE(s2.method_credit_card, '0') as method_credit_card,
		COALESCE(s2.method_dalenys, '0') as method_dalenys,
		COALESCE(s2.method_sepa, '0') as method_sepa,
	--asset investment sums after the store was created
		COALESCE(ai.asset_investment_value, '0') as asset_investment_value,
		COALESCE(ai.asset_investment_count, '0') as asset_investment_count
	from fact_days f 
	left join impressions i 
		on f.reporting_date = i.reporting_date and f.store_id = i.store_id and f.product_sku = i.product_sku 
	left join traffic t 
		on f.reporting_date = t.reporting_date and f.store_id = t.store_id and f.variant_sku = t.variant_sku  
	left join orders os 
		on f.reporting_date = os.reporting_date and f.store_id = os.store_id and f.variant_sku = os.variant_sku   
	left join subscriptions_ s
		on f.reporting_date = s.reporting_date and f.store_id = s.store_id  and f.variant_sku = s.variant_sku 
	left join active_subs_ s2
		on f.reporting_date = s2.reporting_date and f.store_id = s2.store_id  and f.variant_sku = s2.variant_sku 
	left join asset_investment ai 
		on f.reporting_date = ai.reporting_date and f.store_id = ai.store_id and f.variant_sku = ai.variant_sku 
order by 1 desc )
		select *,
		--are all rows zero?
		case when (total_impressions = 0
		and page_views = 0 
		and sessions = 0 
		and users = 0
		and add_to_cart = 0 
		and paid_orders = 0 
		and acquired_subscription = 0 
		and active_subscriptions = 0 
		and asset_investment_value = 0) then 'True'
		else 'False' end as is_all_zero
		from a
		where is_all_zero ='False';

GRANT SELECT ON dwh.international_reporting TO tableau;
