drop table if exists dwh.commercial_targets_daily_store_commercial;
create table dwh.commercial_targets_daily_store_commercial as  
select * 
	from dwh.commercial_targets_daily_store_country 
	where store ='Partnerships'
	  or  store='B2B'
	  or (store='Grover' and country in ('US','DE','EU','ES','AT','NL'));
