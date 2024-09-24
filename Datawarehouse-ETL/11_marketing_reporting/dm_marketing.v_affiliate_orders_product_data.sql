DROP VIEW IF EXISTS dm_marketing.v_affiliate_orders_product_data;
CREATE VIEW dm_marketing.v_affiliate_orders_product_data AS
WITH affiliate_orders AS (
	SELECT order_id,
	  	   affiliate_network,
	   	   affiliate,
	   	   touchpoint,
           order_type
	FROM dm_marketing.v_affiliate_paid_order_publisher_report
)

	SELECT DISTINCT DATE(a.submitted_date) AS reporting_date,
        a.order_id,
        o.affiliate_network,
	   	o.affiliate,
	   	o.touchpoint,
        o.order_type,
	   	a.marketing_channel,
        a.store_country AS country,
        a.submitted_date,
        a.customer_type ,
        a.paid_date,
        c.subscription_id,
        b.category_name,
        b.subcategory_name,
        b.brand,
        b.product_name,
        b.product_sku
    FROM master.order a
    	INNER JOIN affiliate_orders o ON o.order_id = a.order_id
        LEFT JOIN ods_production.order_item b on a.order_id = b.order_id
        LEFT JOIN master.subscription c on c.order_id = b.order_id AND c.product_sku = b.product_sku AND c.variant_sku = b.variant_sku
	WITH NO SCHEMA BINDING;

GRANT SELECT ON TABLE dm_marketing.v_affiliate_orders_product_data TO matillion;
