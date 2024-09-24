drop table if exists staging.tracking_shipment_update;
create table staging.tracking_shipment_update as
with 
stg_fesu_v1 as (
select distinct
	uid,
	max(case when tracking_url <> 'null' then tracking_url end) over (partition by uid) as shipment_id_pre,
	case when shipment_id_pre like '%shipcloud.io%' 
		 then right(shipment_id_pre, len(shipment_id_pre) - 27)
		 when shipment_id_pre like '%myhes.de%'
		 then right(shipment_id_pre, len(shipment_id_pre) - 29)
		 end as shipment_id,
	max(case when tracking_number <> 'null' then tracking_number end) over (partition by uid) as tracking_number ,
	coalesce (nullif(status, ''), nullif(latest_lifecycle_event_name, '')) as status,
	case when (status = 'created' and latest_lifecycle_event_description = 'Shipment created') or
			  (status = 'label created' and latest_lifecycle_event_description = 'Received tracking link from Hermes 2MH')
		 then row_number() over (partition by uid, latest_lifecycle_event_description order by latest_lifecycle_event_timestamp asc)
		 end as idx,
	latest_lifecycle_event_description as details, 
	latest_lifecycle_event_location as location,
	latest_lifecycle_event_timestamp as event_timestamp,
    type as shipment_type,
	carrier ,
	reciever_address_country_iso as shipping_country
from staging.fulfillment_eu_shipment_update_v1 
where latest_lifecycle_event_timestamp <> 'string'  and latest_lifecycle_event_timestamp <>''
),
shipclouds_to_be_excluded as (
	select distinct tracking_id from staging.sf_ops_asset_delivery  
	union 
	select distinct tracking_id from staging.shipcloud_incoming 
)
select 
	uid as shipment_uid,
	shipment_id as tracking_id,
    shipment_type,
	carrier ,
	tracking_number ,
	shipping_country,
	status ,
	details,
	location,
	event_timestamp::timestamp 
from stg_fesu_v1 
where (idx = 1 or idx is null);