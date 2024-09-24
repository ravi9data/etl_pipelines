DROP TABLE IF EXISTS ods_production.addon_35up;
CREATE TABLE ods_production.addon_35up AS 
WITH addons AS 
(
SELECT
	a.id,
	a.external_id,
	a.externaladdoncontractid,
	a.price_in_cents,
	a."name",
	b.order_id,
	b.external_order_id,
	b.trace_id,
	b.store_code,
	c.refund_date ,
	c.refund_amount
FROM
	oltp_addons.contract_line_item a
LEFT JOIN 
	oltp_addons.external_addon_contract b 
ON
	a.externaladdoncontractid = b.id
LEFT JOIN 
	ods_production.payment_addon_35up c 
ON
	a.id = c.addon_id 
)
SELECT
	o.order_id,
	a.id AS addon_id,
	a.external_id,
	a.externaladdoncontractid,
	a."name" AS addon_name,
	o.customer_id,
	o.created_date,
	o.submitted_date,
	o.paid_date,
	o.status,
	a.price_in_cents / 100.0 AS addon_price,
	o.order_value,
	a.refund_date ,
	a.refund_amount, 
	o.new_recurring,
	o.store_country,
	a.store_code,
	o.customer_type,
	o.order_item_count,
	a.external_order_id,
	a.trace_id
FROM
	master.ORDER o
INNER JOIN 
	addons a
ON 
	o.order_id = a.order_id
;
