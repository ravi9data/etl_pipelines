CREATE OR REPLACE VIEW dm_b2b.v_weekly_monthly_reporting AS
WITH customers as (
	select distinct datum,
	d.day_is_last_of_month, 
	case	
		when d.week_day_number = 7 then 1
		when d.datum = current_date then 1
		else 0 end as day_is_end_of_week,
	o.customer_id,
	case when coalesce(paid_orders, 0) > 1 then 'Recurring Customer'
	else 'New Customer' end as new_recurring_customer
	from public.dim_dates d
	left join (select 
		 	d.datum as fact_date,
			o.customer_id, 
			COALESCE (count(case when paid_date is not null then order_id end),0) as paid_orders
	from public.dim_dates d
	left join master.order o
		on d.datum >= o.created_date::DATE 
	WHERE customer_type = 'business_customer'
	group by 1, 2
	) AS o 
	on fact_date = datum
	where datum <= current_date
      and datum >= date_trunc('month', dateadd('month', -13, current_date))
)
,country_ as 
(select 
					distinct customer_id, 
					country_name as first_paid_country, 
					row_number () over (partition by customer_id order by s.start_date asc) as idx
				from master.subscription s
				where s.customer_type ='business_customer'
				and start_date is not null)
, cancellation AS (
SELECT 
	d.datum
    ,d.customer_id
    ,d.day_is_last_of_month
	,d.day_is_end_of_week
	,d.new_recurring_customer
	,count(DISTINCT s.subscription_id) AS cancelled_subscriptions
	,sum(s.subscription_value) AS cancelled_subscription_value
	,count(DISTINCT CASE WHEN s.cancellation_reason_new = 'FAILED DELIVERY' THEN s.subscription_id END) AS cancelled_subscriptions_failed_delivery
	,sum(CASE WHEN s.cancellation_reason_new = 'FAILED DELIVERY' THEN s.subscription_value ELSE 0 END) AS cancelled_subscription_value_failed_delivery
FROM customers d 
inner JOIN master.subscription s
  ON d.datum =  s.cancellation_date::date
  AND d.customer_id = s.customer_id
GROUP BY 1, 2, 3, 4, 5
)
, active_info AS (
SELECT 
	d.datum
	,d.customer_id
	,d.new_recurring_customer
	,sum(coalesce(active_subscriptions,0)) as active_subscriptions
	,count(distinct s.customer_id) as active_customers
	,sum(coalesce(active_subscription_value,0)) as active_subscription_value
	,sum(coalesce(active_committed_subscription_value,0)) as active_committed_subscription_value 
FROM customers d
inner JOIN ods_finance.active_subscriptions_overview s
  ON d.datum = s.fact_date 
  AND d.customer_id = s.customer_id
GROUP BY 1, 2,3
)
, new_subs AS (
SELECT 
	d.datum
    ,d.customer_id
    ,d.new_recurring_customer
	,count(DISTINCT s.subscription_id) AS new_subscriptions
	,sum(s.subscription_value_euro) AS new_subscription_value
	,sum(s.committed_sub_value) AS committed_subscription_revenue
FROM customers d 
inner JOIN ods_production.subscription s
  ON d.datum = s.start_date::date
  AND d.customer_id = s.customer_id
GROUP BY 1, 2, 3
)
, _churn as (
select distinct 
c.datum,	
c.customer_id,
case when active_subscriptions = 0 then true else false end as churn_date
from cancellation c
LEFT JOIN active_info ai
  ON ai.datum = c.datum
 AND ai.customer_id = c.customer_id
)
SELECT distinct 
    c.datum AS fact_date
    ,c.day_is_last_of_month
	,c.day_is_end_of_week
	,c.new_recurring_customer
	,cc.first_paid_country
    ,case
        when COALESCE(bbfm.is_freelancer, 0) = 1
                 and coalesce(active_subscriptions,0) >= 5 then 'freelancer >= 5'
        when COALESCE(bbfm.is_freelancer, 0) = 1
                 and  coalesce(active_subscriptions,0)  < 5 then 'freelancer < 5'
        when COALESCE(bbfm.is_freelancer, 0) = 0
                 and  coalesce(active_subscriptions,0)  >= 5 then 'non freelancer >= 5'
        when COALESCE(bbfm.is_freelancer, 0) = 0
                 and  coalesce(active_subscriptions,0)  < 5 then 'non freelancer < 5'
        else 'validate'
     END AS b2b_classification
     ,case
        when COALESCE(bbfm.is_freelancer, 0) = 0
                 and  active_subscriptions  >= 5 then 'High Value'
        else 'Low Value'
     END AS b2b_logic
     --ACTIVE
    ,COALESCE(sum(ai.active_subscriptions),0) AS active_subscriptions
    ,COALESCE(sum(ai.active_subscription_value),0) AS active_subscription_value
    ,COALESCE(sum(active_subscription_value * 12),0) AS annualized_active_subscription_value
    ,COALESCE(sum(active_committed_subscription_value),0) AS active_committed_subscription_value
    ,COALESCE(sum(active_committed_subscription_value * 12),0) AS annualized_active_committed_subscription_value
    ,COALESCE(sum(active_customers), 0) AS active_customers
        --ACQUIRED 
    ,COALESCE(sum(new_subscriptions), 0) AS new_subscriptions
    ,COALESCE(sum(new_subscription_value), 0) AS new_subscription_value
    --CHURNED
    ,COALESCE(count(distinct ch.customer_id), 0) AS churned_customers
    --CANCELLED
    ,COALESCE(sum(cancelled_subscriptions), 0) AS cancelled_subscriptions
    ,COALESCE(sum(cancelled_subscription_value), 0) AS cancelled_subscription_value
    ,COALESCE(sum(cancelled_subscriptions_failed_delivery), 0) AS cancelled_subscriptions_failed_delivery
    ,COALESCE(sum(cancelled_subscription_value_failed_delivery), 0) AS cancelled_subscription_value_failed_delivery 
FROM customers c
left JOIN ods_production.companies com
  ON c.customer_id = com.customer_id
left join dm_risk.b2b_freelancer_mapping bbfm 
 on bbfm.company_type_name = com.company_type_name 
left JOIN active_info ai
  ON ai.datum = c.datum
 AND ai.customer_id = c.customer_id
 and ai.new_recurring_customer =  c.new_recurring_customer
left JOIN cancellation can 
  ON can.datum = c.datum 
 AND can.customer_id = c.customer_id
left JOIN new_subs ns
  ON ns.datum = c.datum
 AND ns.customer_id = c.customer_id
 left JOIN _churn ch
  ON ch.datum = c.datum
 AND ch.customer_id = c.customer_id
 and churn_date is true
  inner join country_ cc 
  on cc.customer_id = c.customer_id 
  and cc.idx = 1
 GROUP BY 1,2,3,4,5,6,7
WITH NO SCHEMA BINDING;
