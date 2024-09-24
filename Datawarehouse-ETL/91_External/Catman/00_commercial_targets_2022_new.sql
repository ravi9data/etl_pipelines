---TARGETS AS on FEBRUARY 2022

 drop table if exists stg_external_apis.gs_commercial_targets_2022_new ;
 create table stg_external_apis.gs_commercial_targets_2022_new  as 
with remapping as (
 		select 
 			case when store = 'Retail' then 'TOTAL'
	 			when store = 'B2B-Freelancers' then 'Freelancers'
		 		when store = 'B2B-Non-Freelancers' then 'Non Freelancers'
 				else country end as 
 				country,
 			case when store = 'B2C' then 'Grover'
 				when store like '%B2B%' then 'B2B'
 				else store end as store,
 				datum
 			,sum(asv) as asv
 			,sum(acquired_asv) as acquired_asv
 			,sum(cancelled_asv) aS cancelled_asv
 			,SUM(cancelled_subscriptions) AS cancelled_subscriptions
 			,SUM(acquired_subscriptions) AS acquired_subscriptions
 			from dw_targets.gs_commercial_targets_2022
 		group by 1,2,3)
 		,eu as (
 		select 
 			'EU' as country,
 			'Grover' as store,
 			datum,
 			sum(asv) as asv
 			,sum(acquired_asv) as acquired_asv
 			,sum(cancelled_asv) aS cancelled_asv
 			,SUM(cancelled_subscriptions) AS cancelled_subscriptions
 			,SUM(acquired_subscriptions) AS acquired_subscriptions
 			from remapping 
 			where country in ('NL','ES','AT')
 			group by 1,2,3)
 		,total as (	
 			select 'TOTAL' as country,
 			'Grover' as store,
 			datum,
 			sum(asv) as asv
 			,sum(acquired_asv) as acquired_asv
 			,sum(cancelled_asv) aS cancelled_asv
 			,SUM(cancelled_subscriptions) AS cancelled_subscriptions
 			,SUM(acquired_subscriptions) AS acquired_subscriptions
 			from remapping
 			group by 1,2,3)
 		  ,b2btotal as (	
 			select 'TOTAL' as country,
 			'B2B' as store,
 			datum,
 			sum(asv) as asv
 			,sum(acquired_asv) as acquired_asv
 			,sum(cancelled_asv) aS cancelled_asv
 			,SUM(cancelled_subscriptions) AS cancelled_subscriptions
 			,SUM(acquired_subscriptions) AS acquired_subscriptions
 			from remapping
 			where store = 'B2B'
 			group by 1,2,3)	
 			,final_ as (
 			select * from remapping
 			union all 
 			select * from eu 
 			union all 
 			select * from total
 			union all 
 			select * from b2btotal)
 			select 
 				country
 				,store
 				,datum
 				,asv as active_subs_value
 				,NULL as incremental_subs_value
 				,NULL as subs_per_order
 				,NULL as paid_rate
 				,NULL as paid_orders
 				,NULL as incremental_subs
 				,NULL as cancelled_subscriptions
 				, cancelled_asv as cancelled_sub_value
 				,NULL as cancelled_subs_average_value
 				,NULL as approved_orders
 				,NULL as approval_rate
 				,NULL as active_subs
 				,acquired_asv as acquired_subs_value
 				,NULL as acquired_subs_avg_value
 				,acquired_subscriptions as acquired_subs
 				,NULL as cps_excl_branding
 				,NULL as cps
 				,NULL as cpo
 				,NULL as marketing_cost_total
 				,NULL as marketing_cost_branding
 				,NULL as marketing_cost_partnerships
 				,NULL as marketing_cost_performanace
 				,NULL as marketing_cost_vouchers
 				,NULL as marketing_cost_other
 				from final_ ; 
