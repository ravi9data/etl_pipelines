CREATE OR REPLACE VIEW dm_commercial.v_new_portfolio_mapping_overview AS 
SELECT 
	category_name,
	subcategory_name,
	brand,
	product_sku,
	warehouse,
	CASE WHEN mapping_level_2 = 'SOLD' THEN 'SOLD' ELSE mapping_level_1 END AS assets_mapping_level_1,
	mapping_level_2 AS assets_mapping_level_2,
	month_of_last_status_sf_or_im,
	h.date,
	sum(initial_price) AS total_initial_price,
	sum(purchase_price_commercial) AS purchase_price_commercial,
	count(DISTINCT asset_id) AS number_of_assets
FROM public.dim_dates d 
LEFT JOIN dm_commercial.portfolio_overview_sf_and_im_historical h
	ON h.date = d.datum
WHERE assets_mapping_level_1 IS NOT NULL
	AND (d.day_is_last_of_month = 1
		OR d.week_day_number = 7)
GROUP BY 1,2,3,4,5,6,7,8,9
WITH NO SCHEMA BINDING
;

