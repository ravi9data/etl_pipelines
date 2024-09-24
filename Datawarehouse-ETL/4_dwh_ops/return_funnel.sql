drop table if exists dm_recommerce.return_funnel;

create table dm_recommerce.return_funnel as
with label_created as (
	select
		date_trunc('day', return_shipment_label_created_at) fact_date,
		s.category_name,
		s.subcategory_name,
		count(1) label_created
	from ods_production.allocation  a 
	left join ods_production.subscription s 
	on a.subscription_id = s.subscription_id
	where s.country_name not in ('United States')
	group by 1,2,3) 
, return_picked as (
	select
		date_trunc('day', return_shipment_at) fact_date,
		s.category_name,
		s.subcategory_name,
		count(1) return_picked
	from ods_production.allocation  a 
	left join ods_production.subscription s 
	on a.subscription_id = s.subscription_id
	where s.country_name not in ('United States')
	group by 1,2,3 ) 
, return_delivered as (
	select
		date_trunc('day', coalesce (return_delivery_date, return_delivery_date_old) ) fact_date,
		s.category_name,
		s.subcategory_name,
		count(1) return_delivered
	from ods_production.allocation  a 
	left join ods_production.subscription s 
	on a.subscription_id = s.subscription_id
	where s.country_name not in ('United States')
	group by 1,2,3 )
--Get raw data from Wemalo, each refurbishment process has a unique key called articleid
--In some cases, since serial number is not correct, WHS people updates serial number. 
--Follow updates by "=>" sign in [refurbishmententry_serialnumber] column
, wemalo_base as (
	select 
		articleid , --unique identifier for each refurbushiment process , need to be confirmed
		last_value(serialnumber) over 
			(partition by articleid order by event_timestamp asc
			 rows between unbounded preceding and unbounded following ) as serialnumber,  --Workaround for serial number changes, consider final serial number
		cell,
		convert_timezone('Europe/Berlin', event_timestamp::datetime) as event_timestamp
	from stg_kafka_events_full.stream_wms_wemalo_register_refurbishment )
--Highly possible to have more than one record for each scanning, coming from Kafka. So, consider first event
, wemalo_base_last_sn as (
	select 
		articleid ,
		serialnumber,
		cell,
		min(event_timestamp) as event_timestamp 
	from wemalo_base
	group by 1, 2, 3
	)
, wemalo_ranking as (
	select 
		articleid ,
		event_timestamp ,
		serialnumber ,
		cell,
		--dense_rank() over (partition by serialnumber order by articleid::int desc) as article_rank,
		--use LIFO, since we are considering orders in 2021, give 1st rank for latest refurbishment
		case when split_part(lower(cell), '-', 1) = 'spl' and split_part(lower(cell), '-', 2) = 'dr' then 'spl-dr' 
			else split_part(lower(cell), '-', 1) end
		as cell_type,
		row_number() over (partition by articleid , serialnumber , cell_type order by articleid::int, event_timestamp) as cell_type_rank,
		row_number() over (partition by articleid , serialnumber  order by articleid::int, event_timestamp) as basic_rank
	from wemalo_base_last_sn )
, wemalo_final as (
	select 
		date_trunc('day', event_timestamp) fact_date,
		serialnumber,
		a.category_name,
		a.subcategory_name,
		cell,
		case
			when --normal return or direct return
				 cell_type in ('ret1', 'spl-dr') 
			     or 
				 --if it is failed delivery, highly possible that asset returns to position starts with kom/new
				 (cell_type_rank = 1 
			 	   and basic_rank = 1 
				   and (cell_type in ('new') 
				   	    or cell_type like 'kom%')
				 ) 
			then 'Scanned' 
		end as event_rank,
		case when (event_rank is null or event_rank <> 'Scanned') and lag(event_rank) 
					over 
						(partition by articleid order by event_timestamp) = 'Scanned' 
			then 'First Move-' || l.sperrlager_type 
		end as first_move --look for the position right after the scanned position, regardless of which position it is
	from wemalo_ranking  f
	left join stg_external_apis.wemalo_position_labels l on f.cell = l.position
	left join master.asset a on f.serialnumber = a.serial_number
	)
----
----
----end of cte
select 
	fact_date ,
	coalesce (event_rank , first_move) as stage,
	category_name,
	subcategory_name,
	count(1) as value
from wemalo_final 
where coalesce (event_rank , first_move) is not null
group by 1, 2, 3, 4
union 
select 
	fact_date ,
	'Label Created' as stage,
	category_name,
	subcategory_name,
	label_created as value
from label_created 
union 
select 
	fact_date ,
	'Return Picked' as stage,
	category_name,
	subcategory_name,
	return_picked as value
from return_picked 
union 
select 
	fact_date ,
	'Return Delivered' as stage,
	category_name,
	subcategory_name,
	return_delivered as value
from return_delivered;

GRANT SELECT ON dm_recommerce.return_funnel TO tableau;
