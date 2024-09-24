DROP TABLE if exists monitoring.or2;

Create TABLE monitoring.or2 as	
Select
	sea.allocation_id,
	sea.previous_state,
	sea.state,
	sea."date",
	extract(month from sea."date") as month_,
	extract (day from sea."date") as day_,
	a.order_id,
	o.store_id,
	a.return_shipment_label_created_at
From
stg_salesforce_events.allocation sea
inner join  ods_production.allocation a on sea.allocation_id = a.allocation_id
inner join ods_production."order" o on a.order_id = o.order_id
inner join ods_production.store s on o.store_id = s.id
where s.store_type = 'online'
and	sea.state = 'RETURNED'
and previous_state = 'DELIVERED'
and a.return_shipment_tracking_number IS NULL
order by "date" DESC;

GRANT SELECT ON monitoring.or2 TO tableau;
