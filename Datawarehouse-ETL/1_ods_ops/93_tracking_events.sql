drop table if exists ods_operations.tracking_events;
create table ods_operations.tracking_events as
with 
union_sources as (
	select 
		null as shipment_uid,
		tracking_id,
		carrier,
  		shipment_type,
		tracking_number,
		shipping_country,
		status,
		details,
		"location",
		event_timestamp 
	from staging.sf_ops_asset_delivery 
	union
	select 
		null as shipment_uid,
		tracking_id,
		carrier,
  		shipment_type,
		tracking_number,
		shipping_country,
		status,
		details,
		"location",
		event_timestamp 
	from staging.shipcloud_incoming  
	union
	select 
		shipment_uid,
		tracking_id,
		carrier,
  		shipment_type,
		tracking_number,
		shipping_country,
		status,
		details,
		"location",
		event_timestamp
	from staging.tracking_shipment_update 
	where (tracking_id not in 
				(select tracking_id from staging.sf_ops_asset_delivery
				 union 
				 select tracking_id from staging.shipcloud_incoming)	
		  or tracking_id is null)
  ),
details_converted as (
	select 	
      shipment_uid,
      tracking_id,
      carrier,
      shipment_type,
      status,
      tracking_number,
      shipping_country,
      regexp_replace(
      regexp_replace( 
      regexp_replace(
      regexp_replace( 
      regexp_replace(
      regexp_replace(
      regexp_replace( 
      regexp_replace(
      regexp_replace( 
      regexp_replace(
      regexp_replace( 
      regexp_replace( 
      regexp_replace( 
      regexp_replace( 
      regexp_replace( 
	  regexp_replace(
	  regexp_replace(
	  regexp_replace(
      regexp_replace(details, 
          '<a href.*<\/a>', 'Address'),
          'die Filiale\/Agentur gebracht .*', 'die Filiale\/Agentur gebracht Address'),
          'Wunschtag.* wurde','Wunschtag Datum wurde'),
          'Wunschzeitfenster.* wurde','Wunschzeitfenster Datum wurde'),
          'die Zustellung .* beauftragt.','die Zustellung Address beauftragt'),
          'Sendung wurde zugestellt.*','Sendung wurde zugestellt Personnen'),
          'Sendung retour.*','Sendung retour Datum'),
          'ermin .* nicht angetroffen.','ermin Datum nicht angetroffen.'),
          '.* was identified as non-trackable','was identified as non-trackable'),
          'Fahrzeugbeladung: .*','Fahrzeugbeladung: Datum'),
          'Ihre Sendung wird .* zugestellt','Ihre Sendung wird Datum zugestellt'),
          'angegebenen Adresse .* zugestellt.*','angegebenen Adresse Datum zugestellt.'),
          'Ankunft bei Kundenadresse: .*','Ankunft bei Kundenadresse: Datum'),
          'Asset .* returned back','Asset returned back'),
          'Die Abwicklung wird neu verplant.*','Die Abwicklung wird neu verplant Datum'),
          'Die abgeholte Ware wird .* Warenverteilzentrum','Die abgeholte Ware wird Datum Warenverteilzentrum'), 
          'Abgeholte Ware retour am .*', 'Abgeholte Ware retour am Datum'),
          'Ihre Sendung wird am .* abgeholt','Ihre Sendung wird am Datum abgeholt' ),
          'Fake event for .* order', 'Fake event for id order')
      as details,
      "location",
      event_timestamp
	from union_sources
)
select
	coalesce(c.shipment_uid,
             (select shipment_uid from staging.shipment_outbound so
              where so.tracking_id = c.tracking_id
              limit 1),
             (select shipment_uid from staging.shipment_inbound si
              where si.tracking_id = c.tracking_id
              limit 1)
			  ) as shipment_uid,
	c.tracking_id,
	c.carrier,
  	c.shipment_type,
	c.status,
	c.tracking_number,
	c.shipping_country,
    c.details,
    c."location",
    c.event_timestamp,
	t.failed_reason,
    t.event_bucket,
	case when count(1) over (partition by coalesce(shipment_uid, tracking_id)) 
				= row_number () over (partition by coalesce(shipment_uid, tracking_id) order by event_timestamp asc)
		 then true else false end as is_last_event
from details_converted c 
left join staging.tracking_events_mapping t
on c.details = t.details;

--export new tracking events to tracking_events_mapping sheet's New Events tab
drop table if exists hightouch_sources.new_tracking_events;
create table hightouch_sources.new_tracking_events as
select distinct 
	details as new_event
   from ods_operations.tracking_events 
   where details not in ('', '/')
minus
select distinct details from staging.tracking_events_mapping ;

GRANT SELECT ON ods_operations.tracking_events TO operations_redash;
GRANT SELECT ON ods_operations.tracking_events TO cs_redash;
GRANT SELECT ON ods_operations.tracking_events TO GROUP recommerce_data;
GRANT SELECT ON ods_operations.tracking_events TO GROUP recommerce;
GRANT SELECT ON ods_operations.tracking_events TO GROUP risk_users;
