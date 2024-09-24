
/*
 * REFERRAL GUEST ORDER APPROVED 
 */

DROP TABLE IF EXISTS ods_referral.guest_approved_order;
CREATE TABLE ods_referral.guest_approved_order AS 
WITH approved_pre AS (
SELECT 
	guest_id,
	host_id,
	o.order_id,
	transaction_id,
	guest_created_date,
	o.approved_date::timestamp AS approved_date,
	FIRST_VALUE(order_id) OVER(Partition BY guest_id ORDER BY approved_date
								ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW ) AS first_approved_order,
	ROW_NUMBER() OVER(Partition BY guest_id ORDER BY approved_date) AS idx,
	FIRST_VALUE(approved_date::timestamp) OVER(Partition BY guest_id ORDER BY submitted_date
								ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW ) AS first_order_approved_date,
	guest_country
FROM ods_production.ORDER o 
INNER JOIN ods_referral.guest_signup gs 
ON o.customer_id=gs.guest_id
AND approved_date>=guest_created_date)
SELECT 
	guest_id,
	host_id,
	transaction_id,
	order_id,
	first_approved_order,
	guest_created_date,
	approved_date,
	first_order_approved_date,
	guest_country,
	idx
FROM approved_pre;