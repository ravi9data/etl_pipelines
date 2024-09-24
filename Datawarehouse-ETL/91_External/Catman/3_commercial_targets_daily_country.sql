drop table if exists dwh.commercial_targets_daily_country;
create table dwh.commercial_targets_daily_country as  
select distinct 
 case 
  when country ='Total' 
  and store in ('B2B','Partnerships') 
 then 'DE' 
 else country 
 end as country,
 datum,
 sum(active_sub_value_daily_target) as active_sub_value_daily_target
from dwh.commercial_targets_daily_store_country 
where store ='Partnerships'
	  or  store='B2B'
	  or (store='Grover' and country in ('US','DE','EU'))
group by 1,2
;

