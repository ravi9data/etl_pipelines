CREATE or replace view dm_b2b.v_key_accounts as 
select distinct 
fact_date as date, 
c.customer_id,
c.company_name, 
c.company_type_name, 
u.full_name as account_owner, 
sum(aso.active_subscription_value) as active_subscription_value
from master.customer c  
left join ods_b2b.account b
		on c.customer_id = b.customer_id 
left join ods_b2b."user" u 
on b.account_owner = u.user_id
left join ods_finance.active_subscriptions_overview aso 
on c.customer_id = aso.customer_id 
left join public.dim_dates d 
on d.datum = aso.fact_date
where c.customer_type ='business_customer'
and b.key_account__c 
and (d.day_is_last_of_month or aso.fact_date = current_date)
group by 1, 2, 3, 4, 5 
WITH NO SCHEMA BINDING;
