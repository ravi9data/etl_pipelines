drop table if exists dwh.b2b_list_and_orders;
create table dwh.b2b_list_and_orders as 

with x as
( select minimum_cancellation_date, customer_id, count(DISTINCT(subscription_id)) as subs_due_at_next_cancellation
	from master.subscription 
group by 1,2
),
        a as (
	select customer_id,
	paid_subscriptions,
	LAST_VALUE(paid_subscriptions ignore nulls) over (partition by customer_id order by paid_subscriptions ASC rows between unbounded preceding and unbounded following) as max_paid_subs,
	sum(outstanding_asset_value) as outstanding_asset_value,
 	sum(outstanding_residual_asset_value) as outstanding_market_value 
	from master.subscription 
	group by 1,2),
        b as (
	select customer_id, max_paid_subs,
	sum(a.outstanding_asset_value) as outstanding_asset_value,
 	sum(a.outstanding_market_value) as outstanding_market_value 
	from a 
	group by 1,2)
SELECT 
	c.customer_id,
	c.created_at,
        c.updated_at,
    c2.company_id,
    c2.lead_type,
	c.company_name,
	c.first_name,
	c.last_name,
	c.email,
	c.phone_number,
	c.subscription_limit,
	CASE
		when c.subscription_limit is null or c.subscription_limit = 0 then 'Rejected/Pending'
		else 'Approved'
		end as status_business_customer,		
    cc.minimum_cancellation_date,
	cc.subscriptions,
	cc.active_subscriptions,
	cc.active_subscription_value,
        cc.clv,
	cc.committed_subscription_value,
        b.max_paid_subs,
        b.outstanding_asset_value,
        b.outstanding_market_value,
	x.subs_due_at_next_cancellation,
    cc.completed_orders,
    cc.paid_orders,
    cc.declined_orders,
    cc.last_order_created_date,
    CASE
    	WHEN cc.subs_24m >0 Then 24
    	when cc.subs_12m>0 then 12
    	when cc.subs_6m>0 then 6
    	when cc.subs_3m>0 then 3
    	when cc.subs_1m>0 then 1
    	when cc.subs_pag>0 then 0
    	else NULL
    end as max_sub_plan,
     cc.billing_city,
     cc.company_status,
	 cc.company_type_name,
cc.crm_label
   FROM master.customer cc 
   left join ods_data_sensitive.customer_pii c 
    on c.customer_id=cc.customer_id
    left join ods_production.companies c2 
    on c2.customer_id = cc.customer_id 
    left join b 
     on b.customer_id = cc.customer_id
   left join x 
    on c.customer_id=x.customer_id 
    and cast(x.minimum_cancellation_date as date)=cast(cc.minimum_cancellation_date as date)
  where c.customer_type='business_customer';