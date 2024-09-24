CREATE or replace view dm_b2b.v_asv_monitoring as 
select 
	f.datum as fact_date,
	s.customer_id,
	case 
		when s.subscription_id ilike 'F%' then 'New Infra'
		when s.subscription_id ilike 'G%' then 'Migrated - New Infra'
		when s.subscription_id not ilike 'M%' then 'Salesforce'
		else 'check' end as source_,
	a.account_name as account_name, 
	a.key_account__c as is_key_account,
	u.full_name  as account_manager,
	coalesce(sum(s.subscription_value_eur),0) as asv,
	count (distinct s.subscription_id) as active_subscriptions
from public.dim_dates f
left join ods_production.subscription_phase_mapping s 
 ON f.datum::date >= s.fact_day::date
		AND F.datum::date <= COALESCE(s.end_date::date, f.datum::date + 1)
left join ods_b2b.account a
	on s.customer_id = a.customer_id 
left join ods_b2b."user" u 
    on a.account_owner = u.user_id
where f.datum BETWEEN current_date -7 and current_date
and s.customer_type ='business_customer'
group by 1, 2, 3, 4, 5, 6 
WITH NO SCHEMA BINDING;