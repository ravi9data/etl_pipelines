drop table if exists dm_debt_collection.subscription_overview;
create table dm_debt_collection.subscription_overview as
select 
	s.*,
case 
	when status = 'CANCELLED' and dpd<=0 then 'EXCLUDE' 
	else 'INCLUDE' 
end as cancelation_exclusion,
case
	when dpd <= 0 then 'PERFORMING'
	when dpd<31 then 'OD 1-30'
	when dpd<61 then 'OD 31-60'
	when dpd<91 then 'OD 61-90'
	when dpd<121 then 'OD 91-120'
	when dpd<151 then 'OD 121-150'
	when dpd<181 then 'OD 151-180'
	when dpd<361 then 'OD 181-360'
	when dpd<721 then 'OD 361-720'
	when dpd>=721 then 'OD 720+'
	when dpd is null and date_trunc('month', date) = date_trunc('month', start_date::date) then 'IMMATURE SUBSCRIPTION'
	else 'ERROR'
end as dpd_bucket,
case 
	when outstanding_assets = 0 then 'Returned' 
	else 'Outstanding' 
end as asset_status
FROM 
	master.subscription_historical s
where 
	date = last_day(date)
	and date >= '2019-02-01'
;


drop table if exists dm_debt_collection.subscription_roll_rates;
create table dm_debt_collection.subscription_roll_rates as
with prev_month as 
(
	SELECT 
	*
	from 
	dm_debt_collection.subscription_overview
) 
select
sh.*,
sh.dpd_bucket  as current_dpd_bucket,
sh2.dpd_bucket as prev_dpd_bucket,
--B0-B1
case 
	when prev_dpd_bucket in ('PERFORMING','IMMATURE SUBSCRIPTION') then 1 
	else 0 
end as roll_b0_b1_den,
case 
	when prev_dpd_bucket in ('PERFORMING','IMMATURE SUBSCRIPTION') and current_dpd_bucket = 'OD 1-30' then 1 
	else 0 
end as roll_forward_b0_b1_num,
case	
	when roll_forward_b0_b1_num = 1 then 1 
	when prev_dpd_bucket not in ('ERROR','PERFORMING','IMMATURE SUBSCRIPTION') and current_dpd_bucket = 'PERFORMING' then -1
	else 0 
end as net_flow_b0_b1_num,
--B1-B2
case 
	when prev_dpd_bucket = 'OD 1-30' then 1 
	else 0 
end as roll_b1_b2_den,
case 
	when prev_dpd_bucket in ('PERFORMING','IMMATURE SUBSCRIPTION','OD 1-30') and current_dpd_bucket = 'OD 31-60' then 1 
	else 0 
end as roll_forward_b1_b2_num,
case	
	when roll_forward_b1_b2_num = 1 then 1 
	when prev_dpd_bucket not in ('ERROR','PERFORMING','IMMATURE SUBSCRIPTION','OD 1-30') and current_dpd_bucket = 'OD 1-30' then -1
	else 0 
end as net_flow_b1_b2_num,
--B2-B3
case 
	when prev_dpd_bucket = 'OD 31-60' then 1 
	else 0 
end as roll_b2_b3_den,
case 
	when prev_dpd_bucket in ('PERFORMING','IMMATURE SUBSCRIPTION','OD 1-30', 'OD 31-60') and current_dpd_bucket = 'OD 61-90' then 1 
	else 0 
end as roll_forward_b2_b3_num,
case	
	when roll_forward_b2_b3_num = 1 then 1 
	when prev_dpd_bucket not in ('ERROR','PERFORMING','IMMATURE SUBSCRIPTION','OD 1-30','OD 31-60') and current_dpd_bucket = 'OD 31-60' then -1
	else 0 
end as net_flow_b2_b3_num,
--B3-B4
case 
	when prev_dpd_bucket = 'OD 61-90' then 1 
	else 0 
end as roll_b3_b4_den,
case 
	when prev_dpd_bucket in ('PERFORMING','IMMATURE SUBSCRIPTION','OD 1-30', 'OD 31-60','OD 61-90') and current_dpd_bucket = 'OD 91-120' then 1 
	else 0 
end as roll_forward_b3_b4_num,
case	
	when roll_forward_b3_b4_num = 1 then 1 
	when prev_dpd_bucket not in ('ERROR','PERFORMING','IMMATURE SUBSCRIPTION','OD 1-30','OD 31-60','OD 61-90') and current_dpd_bucket = 'OD 61-90' then -1
	else 0 
end as net_flow_b3_b4_num,
--B4-B5
case 
	when prev_dpd_bucket = 'OD 91-120' then 1 
	else 0 
end as roll_b4_b5_den,
case 
	when prev_dpd_bucket in ('PERFORMING','IMMATURE SUBSCRIPTION','OD 1-30', 'OD 31-60','OD 61-90','OD 91-120') and current_dpd_bucket = 'OD 121-150' then 1 
	else 0 
end as roll_forward_b4_b5_num,
case	
	when roll_forward_b4_b5_num = 1 then 1 
	when prev_dpd_bucket not in ('ERROR','PERFORMING','IMMATURE SUBSCRIPTION','OD 1-30','OD 31-60','OD 61-90','OD 91-120') and current_dpd_bucket = 'OD 121-150' then -1
	else 0 
end as net_flow_b4_b5_num,
--B5-B6
case 
	when prev_dpd_bucket = 'OD 121-150' then 1 
	else 0 
end as roll_b5_b6_den,
case 
	when prev_dpd_bucket in ('PERFORMING','IMMATURE SUBSCRIPTION','OD 1-30', 'OD 31-60','OD 61-90','OD 91-120','OD 121-150') and current_dpd_bucket = 'OD 151-180' then 1 
	else 0 
end as roll_forward_b5_b6_num,
case	
	when roll_forward_b5_b6_num = 1 then 1 
	when prev_dpd_bucket not in ('ERROR','PERFORMING','IMMATURE SUBSCRIPTION','OD 1-30','OD 31-60','OD 61-90','OD 91-120','OD 121-150') and current_dpd_bucket = 'OD 151-180' then -1
	else 0 
end as net_flow_b5_b6_num,
--B6-B7+
case 
	when prev_dpd_bucket ='OD 151-180' then 1 
	else 0 
end as roll_b6_b7_den,
case 
	when prev_dpd_bucket in ('PERFORMING','IMMATURE SUBSCRIPTION','OD 1-30', 'OD 31-60','OD 61-90','OD 91-120','OD 121-150','OD 151-180') and current_dpd_bucket in ('OD 181-360' , 'OD 361-720', 'OD 720+') then 1 
	else 0 
end as roll_forward_b6_b7_num,
case	
	when roll_forward_b6_b7_num = 1 then 1 
	when prev_dpd_bucket not in ('ERROR','PERFORMING','IMMATURE SUBSCRIPTION','OD 1-30','OD 31-60','OD 61-90','OD 91-120','OD 121-150','OD 151-180') and current_dpd_bucket = 'OD 151-180' then -1
	else 0 
end as net_flow_b6_b7_num
FROM 
	dm_debt_collection.subscription_overview sh 
		left join
		prev_month sh2 
		on sh.subscription_id = sh2.subscription_id 
		and sh2.date = date_trunc('month',sh.date)-1
where sh.date >= '2019-04-30'

;

GRANT SELECT ON dm_debt_collection.subscription_roll_rates TO tableau;
GRANT SELECT ON dm_debt_collection.subscription_overview TO tableau;
