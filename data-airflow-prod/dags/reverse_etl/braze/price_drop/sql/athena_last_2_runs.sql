WITH ordered_data AS (
SELECT 
    external_id,
    mtl_cart_price_drop_names,
    mtl_cart_price_drop_full_prices,
    mtl_cart_price_drop_sale_prices,
    mtl_cart_price_drop_links,
    mtl_cart_price_drop_variant_sku,
    mtl_wishlist_price_drop_names,
    mtl_wishlist_price_drop_full_prices,
    mtl_wishlist_price_drop_sale_prices,
    mtl_wishlist_price_drop_images,
    mtl_wishlist_price_drop_links,
    mtl_wishlist_price_drop_variant_sku,
    updated_at,
    ROW_NUMBER () OVER (PARTITION BY external_id ORDER BY updated_at DESC ) as rn
FROM "braze_price_drop_agg_historical"
),

last_run AS (
SELECT
    external_id,
    mtl_cart_price_drop_names,
    mtl_cart_price_drop_full_prices,
    mtl_cart_price_drop_sale_prices,
    mtl_cart_price_drop_links,
    mtl_cart_price_drop_variant_sku,
    mtl_wishlist_price_drop_names,
    mtl_wishlist_price_drop_full_prices,
    mtl_wishlist_price_drop_sale_prices,
    mtl_wishlist_price_drop_images,
    mtl_wishlist_price_drop_links,
    mtl_wishlist_price_drop_variant_sku,
    updated_at
FROM ordered_data
WHERE rn=1),

prev_run AS (
SELECT
    external_id,
    mtl_cart_price_drop_names,
    mtl_cart_price_drop_full_prices,
    mtl_cart_price_drop_sale_prices,
    mtl_cart_price_drop_links,
    mtl_cart_price_drop_variant_sku,
    mtl_wishlist_price_drop_names,
    mtl_wishlist_price_drop_full_prices,
    mtl_wishlist_price_drop_sale_prices,
    mtl_wishlist_price_drop_images,
    mtl_wishlist_price_drop_links,
    mtl_wishlist_price_drop_variant_sku,
    updated_at
FROM ordered_data
WHERE rn=2)

SELECT
	l.external_id,                                  
	l.mtl_wishlist_price_drop_names AS mtl_wishlist_price_drop_names_last,
	p.mtl_wishlist_price_drop_names AS mtl_wishlist_price_drop_names_prev,
	l.mtl_wishlist_price_drop_full_prices AS mtl_wishlist_price_drop_full_prices_last,
	p.mtl_wishlist_price_drop_full_prices AS mtl_wishlist_price_drop_full_prices_prev,
	l.mtl_wishlist_price_drop_sale_prices AS mtl_wishlist_price_drop_sale_prices_last,
	p.mtl_wishlist_price_drop_sale_prices AS mtl_wishlist_price_drop_sale_prices_prev,
	l.mtl_wishlist_price_drop_links AS mtl_wishlist_price_drop_links_last,
	p.mtl_wishlist_price_drop_links AS mtl_wishlist_price_drop_links_prev,
    l.mtl_wishlist_price_drop_variant_sku AS mtl_wishlist_price_drop_variant_sku_last,
    p.mtl_wishlist_price_drop_variant_sku AS mtl_wishlist_price_drop_variant_sku_prev,
	l.mtl_cart_price_drop_names AS mtl_cart_price_drop_names_last,
	p.mtl_cart_price_drop_names AS mtl_cart_price_drop_names_prev,
	l.mtl_cart_price_drop_full_prices AS mtl_cart_price_drop_full_prices_last,
	p.mtl_cart_price_drop_full_prices AS mtl_cart_price_drop_full_prices_prev,
	l.mtl_cart_price_drop_sale_prices AS mtl_cart_price_drop_sale_prices_last,
	p.mtl_cart_price_drop_sale_prices AS mtl_cart_price_drop_sale_prices_prev,
	l.mtl_cart_price_drop_links AS mtl_cart_price_drop_links_last,
	p.mtl_cart_price_drop_links AS mtl_cart_price_drop_links_prev,
    l.mtl_cart_price_drop_variant_sku AS mtl_cart_price_drop_variant_sku_last,
    p.mtl_cart_price_drop_variant_sku AS mtl_cart_price_drop_variant_sku_prev
FROM
	last_run AS l 
	LEFT JOIN prev_run AS p ON l.external_id=p.external_id;
