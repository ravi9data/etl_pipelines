DROP VIEW IF EXISTS dm_catman.v_add_on_US;
CREATE VIEW dm_catman.dm_catman.v_add_on_US AS
WITH base_data AS
(
SELECT 
order_id 
,event_time_add_to_cart AS event_time_add_to_cart
, event_time_product_added AS event_time_product_added 
, product_sku AS primary_sku
, product_sku_product_added AS product_sku_added
FROM dm_product.v_conversion_add_on_report
WHERE product_sku_product_added IS NOT NULL
)
,product_list AS (
SELECT 
sku AS primary_sku
,ROW_NUMBER() OVER (PARTITION BY sku ORDER BY id) AS sheet_id /*duplicates*/
,pl."add-on 1 sku" AS add_sku_1
,pl."add-on 2 sku" AS add_sku_2
,pl."add-on 3 sku" AS add_sku_3
,pl."add-on 4 sku" AS add_sku_4
,pl."add-on 5 sku" AS add_sku_5
,pl."add-on 6 sku" AS add_sku_6
FROM dm_catman.us_product_list pl
)
SELECT 
adr.order_id
,adr.event_time_add_to_cart
, adr.event_time_product_added 
, adr.primary_sku
, adr.product_sku_added
, ROW_NUMBER() OVER (PARTITION BY adr.order_id ORDER BY adr.event_time_product_added) AS add_rnk /*incase bundle*/
, (CASE 
	WHEN adr.product_sku_added = pl.add_sku_1 THEN 'add_sku_1'
	WHEN adr.product_sku_added = pl.add_sku_2 THEN 'add_sku_2'
	WHEN adr.product_sku_added = pl.add_sku_3 THEN 'add_sku_3'
	WHEN adr.product_sku_added = pl.add_sku_4 THEN 'add_sku_4'
	WHEN adr.product_sku_added = pl.add_sku_5 THEN 'add_sku_5'
	WHEN adr.product_sku_added = pl.add_sku_6 THEN 'add_sku_6'
END) AS add_sku_info
FROM base_data adr
INNER JOIN product_list pl 
ON adr.primary_sku = pl.primary_sku
AND (adr.product_sku_added = pl.add_sku_1
OR adr.product_sku_added = pl.add_sku_2
OR adr.product_sku_added = pl.add_sku_3
OR adr.product_sku_added = pl.add_sku_4
OR adr.product_sku_added = pl.add_sku_5
OR adr.product_sku_added = pl.add_sku_6
)
WHERE event_time_add_to_cart IS NOT NULL
AND sheet_id = 1 /* remove duplicates */
WITH NO SCHEMA BINDING;