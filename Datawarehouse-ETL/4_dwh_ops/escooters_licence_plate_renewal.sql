CREATE TEMP TABLE tmp_escooters_licence_plates_renewal AS 
WITH prep AS (
	SELECT
		COALESCE(s.subscription_bo_id,s.subscription_sf_id,s.subscription_id) AS subscription_id_ops,
		s.customer_id,
		c.company_name, 
		s.minimum_cancellation_date::date, 
		a.asset_id,
		a.allocation_id AS asset_allocation_id,
		s.country_name, 
		CASE 
			WHEN current_date > s.minimum_cancellation_date 
				THEN 'B'
			WHEN s.minimum_cancellation_date::date <= dateadd('day',-1,dateadd('month',2,date_trunc('month',current_date)))::date 
				THEN 'C'
			ELSE 'A'	
		END AS priority,
		ROW_NUMBER () OVER (PARTITION BY a.subscription_id ORDER BY a.allocated_at DESC) AS rowno
	FROM master.subscription s 
	LEFT JOIN master.allocation a 
		ON a.subscription_id = s.subscription_id
	LEFT JOIN master.customer c
		ON c.customer_id = s.customer_id 
	WHERE s.status = 'ACTIVE'
		AND s.subcategory_name = 'Scooters'
		AND a.allocation_status_original NOT IN ('RETURNED', 'CANCELLED', 'SOLD')
)
SELECT 
	ROW_NUMBER () OVER (ORDER BY p.subscription_id_ops) AS order_,
	p.priority,
	current_date AS added_on,
	a.asset_status_original AS product_status,
	a.variant_sku,
	p.asset_allocation_id,
	country_name AS country,
	minimum_cancellation_date,
	a.product_name,
	a.serial_number AS vin,
	p.subscription_id_ops,
	c.first_name||' '||c.last_name AS full_name,
	c.email,
	c.street||' '||c.house_number AS street,
	c.shipping_zip AS postal_code,
	initcap(c.shipping_city) AS city,
	c.customer_type,
	p.company_name
FROM prep p
LEFT JOIN ods_data_sensitive.customer_pii c 
	ON p.customer_id = c.customer_id 
LEFT JOIN master.asset a
		ON p.asset_id = a.asset_id
WHERE p.rowno = 1
	AND a.asset_status_original = 'ON LOAN'
;

BEGIN TRANSACTION;

INSERT INTO hightouch_sources.escooters_licence_plates_renewal
SELECT *
FROM tmp_escooters_licence_plates_renewal 
WHERE subscription_id_ops NOT IN (SELECT subscription_id_ops FROM hightouch_sources.escooters_licence_plates_renewal)
;

END TRANSACTION;

DROP TABLE IF EXISTS tmp_escooters_licence_plates_renewal;
