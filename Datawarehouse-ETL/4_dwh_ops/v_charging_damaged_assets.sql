-- dm_operations.v_charging_damaged_assets source

CREATE OR REPLACE VIEW dm_operations.v_charging_damaged_assets
AS --DROP TABLE IF EXISTS dm_operations.charging_damaged_assets;
CREATE VIEW dm_operations.v_charging_damaged_assets AS 
with return_orders as (
	select 
		o.order_number,
		o.serial_number,
		o.new_serial_number,
		a.allocation_id,
		a.asset_id,
		o.return_confirmation_at as returned_at,
		coalesce (o.available_for_repair_at, o.sent_to_repair_at)  as repair_decided_at,
		coalesce (o.available_for_recommerce_at, o.sent_to_recommerce_at) as recommerce_decided_at,
		o.locked_at 
	from ods_operations.ingram_micro_orders o 
	left join ods_operations.ingram_allocation_mapping a 
	on o.order_number = a.order_number
	where returned_at > '2023-12-12'
),
payments as (
	select 
		allocation_id ,
		status,
		amount_paid,
		amount_due,
		due_date,
		paid_date
	from ods_production.payment_asset 
	where created_at > '2023-12-12'
	and payment_type = 'REPAIR COST'
)
select 
	o.order_number,
	o.returned_at,
	o.serial_number ,
	o.new_serial_number ,
	o.allocation_id ,
	o.asset_id,
	case when o.recommerce_decided_at is not null then 'Recommerce'
		 when o.repair_decided_at is not null then 'Repair'
		 when o.locked_at is not null then 'Locked'
		 else 'Other' end as decision,
	case when sa.allocation_id is not null then 'Uploaded' 
		 else 'No' end is_uploaded,
	case when sa.due_date::date <= current_date then 'Due'
		 else 'No' end is_due,
	case when sa.paid_date IS NOT NULL then 'Received'
		 else 'No' end is_received,
	amount_paid,
	amount_due
from return_orders o 
left join payments sa 
on o.allocation_id = sa.allocation_id
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_operations.v_charging_damaged_assets TO operations_redash;
