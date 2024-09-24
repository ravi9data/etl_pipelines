
drop table if exists ods_production.subscription_active_label;
create table ods_production.subscription_active_label as
select  
 	 s.subscription_id, s.customer_id,s.start_date,s.allocation_status,s.order_id,
	CASE when sa.allocated_assets = 1 
	 then  
	 	case when sa.allocated_assets > 0
			 and sa.delivered_assets > 0
			 and sa.returned_assets < sa.delivered_assets
			 and sc.paid_subscriptions > s.minimum_term_months
			 then 'PAST RENTAL PLAN'
		when sa.allocated_assets > 0
			 and sa.delivered_assets > 0
			 and sa.returned_assets < sa.delivered_assets
			 and sc.paid_subscriptions > 1
			 then 'PAST REVOCATION'
		when sa.allocated_assets > 0
			 and sa.delivered_assets > 0
			 and sa.returned_assets < sa.delivered_assets
			 and sc.paid_subscriptions = 1
			 then 'DELIVERED'
		when sa.allocated_assets > 0
			 and sa.delivered_assets = 0
			 and sa.shipped_assets > 0
			then 'SHIPPED'
		when sa.allocated_assets > 0
				then 'ALLOCATED'
					end
		when sa.allocated_assets is null 
				then 'ACTIVE NOT ALLOCATED'  
		else 'MULTIPLE ALLOCATIONS'
		end as sub_label
   from ods_production.subscription s 
    LEFT JOIN ods_production.subscription_cashflow sc 
     ON s.subscription_id = sc.subscription_id
    LEFT JOIN ods_production.subscription_assets sa 
     ON s.subscription_id = sa.subscription_id
	 WHERE s.status = 'ACTIVE'
	 GROUP BY 1,2,3,4,5,6
	 
	 