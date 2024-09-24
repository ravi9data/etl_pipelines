drop table if exists dwh.asset_capitalsource_sold_date;
create table dwh.asset_capitalsource_sold_date as
with a as (
		select *,  
		case when new_value = 'Grover Finance I GmbH' and old_value = 'Grover Finance II GmbH' then
			case when DATE_TRUNC('month',created_at) = '2021-08-01' then '2021-08-01 00:01:00'::timestamp
			when DATE_TRUNC('month',created_at) = '2021-09-01' then '2021-09-01 00:01:00'::timestamp end 
				end as capital_source_sold_date
				from 
		dwh.asset_capitalsource_change 
		order by created_at desc 
		)
		select 
			* from a;