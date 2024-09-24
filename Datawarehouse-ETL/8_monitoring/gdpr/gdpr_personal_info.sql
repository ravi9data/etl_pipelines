
--providing personal_info to google sheets
DELETE FROM hightouch_sources.gdpr_personal_info;

INSERT INTO hightouch_sources.gdpr_personal_info 
WITH capital_source AS (
	SELECT
        customer_id, 
        listagg(distinct (CASE WHEN pa.capital_source in ('Grover Finance II GmbH','Grover Finance I GmbH') then pa.capital_source
                               WHEN pa.capital_source is null then null
                               ELSE null 
                          END) , ', ') 
        as capital_source 
    FROM ods_production.payment_all pa 
    WHERE pa.customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
    GROUP BY 1
)
, addresses AS (
	SELECT 
		customer_id,
		billingcountry || ',' || billingpostalcode || ',' || billingcity || ',' || billingstreet AS billing_address,
		shippingcountry || ',' || shippingpostalcode || ',' || shippingcity || ',' || shippingstreet AS shipping_address,
		ROW_NUMBER() OVER (PARTITION BY customer_id, billing_address, shipping_address ORDER BY order_id DESC) AS rowno
	FROM ods_data_sensitive.customer_order_pii
	WHERE customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
)
SELECT 
	c.customer_id,
	c.birthdate,
	c.first_name,
	c.last_name,
	c.email,
	c.phone_number,
	c.email_subscribe,
	COALESCE(a.billing_address, c.billing_country || ', ' || initcap(c.billing_city) || ', ' || c.billing_zip || ', ' || initcap(c.street) || ' ' || c.house_number) AS billing_address,
	COALESCE(a.shipping_address, c.shipping_country || ', ' || initcap(c.shipping_city) || ', ' || c.shipping_zip || ', ' || initcap(c.street) || ' ' || c.house_number) AS shipping_address,
	pa.capital_source
FROM ods_data_sensitive.customer_pii AS c
LEFT JOIN capital_source AS pa 
  ON pa.customer_id = c.customer_id
LEFT JOIN addresses AS a
  ON a.customer_id = c.customer_id
  AND a.rowno = 1
WHERE c.customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input);
