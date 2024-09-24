DROP TABLE if exists dwh.customer_pmf_survey;
CREATE TABLE dwh.customer_pmf_survey as
with customer_subs as (
select c.customer_id,
count(case when cancellation_reason_new = 'CANCELLED BEFORE SHIPMENT' then subscription_id end) as cancelled_by_us,
count(case when cancellation_reason_new = 'CANCELLED BEFORE ALLOCATION - CUSTOMER REQUEST' then subscription_id end) as cancelled_customer_request,
count(case when cancellation_reason_new in ('CANCELLED BEFORE ALLOCATION - OTHERS','CANCELLED BEFORE ALLOCATION - PROCUREMENT') then subscription_id end)as cancelled_before_allocation,
count(case when cancellation_reason_new = 'REVOCATION' then subscription_id end) as cancelled_revocation
from master.customer c 
left join master.subscription s 
 on s.customer_id = c.customer_id
 group by 1)
, a as (
select 
	c.customer_id, 
	o.order_id,
	o.order_rank,
	s.subscription_id, 
	s.asset_recirculation_status,
	datediff('day',o.created_date::date, c.customer_acquisition_cohort::date) as avg_days_until_first_delivery,
	od.declined_orders,
	od.cancelled_orders,
	o.voucher_code,
	cs.cancelled_by_us,
	cs.cancelled_customer_request,
	cs.cancelled_before_allocation,
        cs.cancelled_revocation
	from master.customer c
	left join master.subscription s
	on s.subscription_id = c.customer_acquisition_subscription_id
	left join ods_production.customer_orders_details od 
	on od.customer_id = c.customer_id
	left join master.order o
		on (c.customer_id = o.customer_id and o.order_rank = 1)
	left join customer_subs cs 
	 on cs.customer_id = c.customer_id)
,pmf as (
select * 
	from stg_external_apis.pfm_survey_result),
customer as (
select 
	c.customer_id as user_id,
	c."date", 
	coalesce(c.active_subscriptions,0) as active_subscriptions,
	c.subscriptions as lifetime_subscriptions,
	c.crm_label,
	c.rfm_segment,
	c.email_subscribe as newsletter, 
	c.clv,
	c.ever_rented_asset_purchase_price,
	c.first_subscription_acquisition_channel,
	c.first_subscription_store,
	c.first_subscription_duration,
	c.first_subscription_product_category,
	c.start_date_of_first_subscription
	from master.customer_historical c)
	,final as (
select 
	distinct (pii.customer_id) as customer__id, 
	pmf.*, 
	c.*,
	a.*
	from ods_data_sensitive.customer_pii pii 
	left join pmf on 
	 pmf.p=pii.email
	left join customer c
	 on c.user_id=pii.customer_id
	 and pmf.submittedat::date=c."date"
	 left join a 
	 on a.customer_id = pii.customer_id
	where pmf.submittedat::date>='2019-05-01')
	select * from final;

GRANT SELECT ON dwh.customer_pmf_survey TO tableau;
