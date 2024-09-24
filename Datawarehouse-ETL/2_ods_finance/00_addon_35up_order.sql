BEGIN;

TRUNCATE TABLE ods_production.addon_35up_order;
INSERT INTO ods_production.addon_35up_order	
WITH addons AS 
(
SELECT
	a.id,
	a.price_in_cents,
	b.order_id,
	b.store_code,
	c.refund_date
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
	o.customer_id,
	o.created_date,
	o.submitted_date,
	o.paid_date,
	o.status,
	o.order_value,
	o.new_recurring,
	o.store_country,
	o.customer_type,
	o.order_item_count,
	a.store_code,
	a.refund_date,
	count(DISTINCT id) AS addon_item_count,
	sum(a.price_in_cents / 100.0) AS addon_price
FROM
	master.ORDER o
INNER JOIN 
	addons a
ON
	o.order_id = a.order_id
GROUP BY 
	1,2,3,4,5,6,7,8,9,10,11,12,13
;

COMMIT;