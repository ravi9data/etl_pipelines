CREATE OR REPLACE VIEW hightouch_sources.product_SKU_attributes AS 
SELECT 
	v.ean,
	v.upcs,
	p.product_sku,
	v.variant_sku,
	p.brand,
	p.product_name,
	v.variant_color,
	sf.category__c,
	d.original_height,
	d.original_width,
	d.original_weight,
	d.original_length,
	sf.device_type__c, 
	p.market_price ,
	v.article_number,
	v.product_id,
	p.created_at
FROM ods_production.variant v
LEFT JOIN ods_production.product p 
	ON v.product_id = p.product_id
LEFT JOIN dm_operations.dimensions d
	ON v.variant_sku = d.variant_sku
LEFT JOIN stg_salesforce.product2 sf
	ON v.variant_sku = sf.sku_variant__c 
WITH NO SCHEMA BINDING;