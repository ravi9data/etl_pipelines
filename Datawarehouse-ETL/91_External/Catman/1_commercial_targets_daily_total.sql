drop table if exists dwh.commercial_targets_daily_total;
create table dwh.commercial_targets_daily_total as  
select * 
	from dwh.commercial_targets_daily_store_country 
	where store='Total'
    and country='Total'
	;
	 