
/*
 * REFERRAL GUEST ORDER SUBMITTED 
 */
	
DROP TABLE IF EXISTS ods_referral.guest_submitted_order;
CREATE TABLE ods_referral.guest_submitted_order AS 
WITH submitted_pre AS (
SELECT 
	guest_id,
	host_id,
	o.order_id,
	transaction_id,
	guest_created_date,
	o.submitted_date::timestamp AS submitted_date,
	FIRST_VALUE(order_id) OVER(Partition BY guest_id ORDER BY submitted_date
								ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW ) AS first_submitted_order,
	ROW_NUMBER() OVER(Partition BY guest_id ORDER BY submitted_date) AS idx,
	FIRST_VALUE(submitted_date::timestamp) OVER(Partition BY guest_id ORDER BY submitted_date
								ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW ) AS first_order_submitted_date,
	guest_country
FROM ods_production.ORDER o 
INNER JOIN ods_referral.guest_signup gs 
ON o.customer_id=gs.guest_id
AND submitted_date>=guest_created_date)
SELECT 
	guest_id,
	host_id,
	transaction_id,
	order_id,
	first_submitted_order,
	guest_created_date,
	submitted_date,
	first_order_submitted_date,
	guest_country,
	idx
FROM submitted_pre;
