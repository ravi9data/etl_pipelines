CREATE OR REPLACE VIEW dm_marketing.v_order_and_product_category AS
WITH sub AS ( 
	SELECT 
		o.order_id,
		s.product_sku ,
		s.category_name ,
		s.price
	FROM master."order" o 
	INNER JOIN ods_production.order_item s 
	  ON o.order_id = s.order_id
	WHERE o.completed_orders = 1 
		AND o.submitted_date::DATE >= CURRENT_DATE - 180
)
, sub_2  AS ( 
	SELECT 
		order_id,
		category_name,
		COUNT(DISTINCT product_sku) AS total_products,
		sum(price) AS total_price
	FROM sub
	GROUP BY 1,2
)
, cat AS (  
	SELECT 
		order_id, 
		category_name, 
		ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY total_price DESC, total_products DESC, category_name) rowno
	FROM sub_2
)
, order_category AS (
	SELECT 
		order_id,
		category_name,
		rowno
		FROM cat 
	WHERE rowno = 1
)

SELECT
    o.*,
    c.category_name,
    COALESCE(CASE
                 WHEN omc.marketing_channel = 'Display' AND marketing_source = 'google'
                     THEN 'Google Display'
                 WHEN omc.marketing_channel = 'Display' AND marketing_source = 'google_discovery'
                     THEN 'Google Discovery'
                 WHEN omc.marketing_channel = 'Display' AND marketing_source = 'outbrain'
                     THEN 'Outbrain'
                 WHEN omc.marketing_channel = 'Display' AND LOWER(marketing_source) = 'youtube'
                     THEN 'Google Youtube'
                 WHEN omc.marketing_channel = 'Display' AND LOWER(marketing_source) = 'google_perf_max'
                     THEN 'Google Performance Max'
                 WHEN omc.marketing_channel = 'Display'
                     THEN 'Display Other'
                 WHEN omc.marketing_channel = 'Paid Search Brand' AND marketing_source = 'bing'
                     THEN 'Bing Search Brand'
                 WHEN omc.marketing_channel = 'Paid Search Brand' AND marketing_source = 'google'
                     THEN 'Google Search Brand'
                 WHEN omc.marketing_channel = 'Paid Search Brand'
                     THEN 'Paid Search Brand Other'
                 WHEN omc.marketing_channel = 'Paid Search Non Brand' AND marketing_source = 'bing'
                     THEN 'Bing Search Non Brand'
                 WHEN omc.marketing_channel = 'Paid Search Non Brand' AND marketing_source = 'google'
                     THEN 'Google Search Non Brand'
                 WHEN omc.marketing_channel = 'Paid Search Non Brand'
                     THEN 'Paid Search Non Brand Other'
                 WHEN omc.marketing_channel = 'Paid Social' AND LOWER(marketing_source) = 'facebook'
                     THEN 'Facebook'
                 WHEN omc.marketing_channel = 'Paid Social' AND marketing_source = 'linkedin'
                     THEN 'Linkedin'
                 WHEN omc.marketing_channel = 'Paid Social' AND marketing_source = 'snapchat'
                     THEN 'Snapchat'
                 WHEN omc.marketing_channel = 'Paid Social' AND marketing_source = 'tiktok'
                     THEN 'TikTok'
                 WHEN omc.marketing_channel = 'Paid Social'
                     THEN 'Paid Social Other'
                 WHEN omc.marketing_channel = 'Shopping' AND marketing_source = 'google'
                     THEN 'Google Shopping'
                 WHEN omc.marketing_channel = 'Shopping' AND marketing_source = 'bing'
                     THEN 'Bing Shopping'
                 WHEN omc.marketing_channel = 'Shopping' AND marketing_source = 'ebay_kleinanzeigen'
                     THEN 'eBay Kleinanzeigen Shopping'
                 WHEN omc.marketing_channel = 'Shopping' AND marketing_source = 'marktplaats'
                     THEN 'Marktplaats Shopping'
                 ELSE omc.marketing_channel
                 END,'n/a') AS marketing_channel_detailed
FROM master."order" o
    INNER JOIN order_category c ON o.order_id = c.order_id
    LEFT JOIN ods_production.order_marketing_channel omc ON omc.order_id = o.order_id
WITH NO SCHEMA BINDING;
