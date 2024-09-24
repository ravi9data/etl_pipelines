drop table if exists ods_data_sensitive.returned_damaged_assets;
create table ods_data_sensitive.returned_damaged_assets as
select 
	a.issue_date,
	cp.first_name,
	cp.last_name,
	cp.email,
	a.order_id,
	s.subscription_id,
	a.return_delivery_date,
	a.refurbishment_start_at,
	s.status AS subscription_status,
	s.cancellation_date, 
	a.issue_comments ,
	a.returned_final_condition,
	a.returned_functional_condition_note,
	a.returned_external_condition_note,
	s.replacement_attempts 
from ods_production.allocation a  
left join ods_production.subscription s 
	on a.subscription_id = s.subscription_id 
left join ods_data_sensitive.customer_pii cp 
	on cp.customer_id = s.customer_id 
where return_delivery_date is not null 
--and total_allocations_per_subscription = 1
and issue_reason = 'Asset Damage'
and a.return_delivery_date > dateadd('year', -3, current_date) 
and replacement_attempts is null;
