WITH cart AS (
SELECT 
	customer_id AS external_id  
	,array_agg(product_name_x ORDER BY product_sku, store_id, source_) AS  mtl_cart_price_drop_names
	,array_agg(old_price ORDER BY product_sku, store_id, source_) AS  mtl_cart_price_drop_full_prices
	,array_agg(current_price ORDER BY product_sku, store_id, source_) AS  mtl_cart_price_drop_sale_prices
	,array_agg(image_url ORDER BY product_sku, store_id, source_) AS  mtl_cart_price_drop_images
	,array_agg(slug ORDER BY product_sku, store_id, source_) AS  mtl_cart_price_drop_links
	,array_agg(product_sku || 'V' || variant_id_x ORDER BY product_sku, variant_id_x, store_id, source_) AS mtl_cart_price_drop_variant_sku
 FROM "braze_price_drop_user_and_product"
WHERE
    source_='Shopping_Cart'
GROUP BY 
	customer_id),
	
wishlist AS (
SELECT 
	customer_id AS external_id  
	,array_agg(product_name_x ORDER BY product_sku, store_id, source_) AS  mtl_wishlist_price_drop_names
	,array_agg(old_price ORDER BY product_sku, store_id, source_) AS  mtl_wishlist_price_drop_full_prices
	,array_agg(current_price ORDER BY product_sku, store_id, source_) AS  mtl_wishlist_price_drop_sale_prices
	,array_agg(image_url ORDER BY product_sku, store_id, source_) AS  mtl_wishlist_price_drop_images
	,array_agg(slug ORDER BY product_sku, store_id, source_) AS  mtl_wishlist_price_drop_links
	,array_agg(product_sku || 'V' || variant_id_x ORDER BY product_sku, variant_id_x, store_id, source_) AS mtl_wishlist_price_drop_variant_sku
 FROM "braze_price_drop_user_and_product"
WHERE
    source_='Wishlist'
GROUP BY 
	customer_id
)

SELECT
    COALESCE(cart.external_id,wishlist.external_id) AS external_id,
    mtl_cart_price_drop_names,
    mtl_cart_price_drop_full_prices,
    mtl_cart_price_drop_sale_prices,
    mtl_cart_price_drop_images,
    mtl_cart_price_drop_links,
    mtl_cart_price_drop_variant_sku,
    mtl_wishlist_price_drop_names,
    mtl_wishlist_price_drop_full_prices,
    mtl_wishlist_price_drop_sale_prices,
    mtl_wishlist_price_drop_images,
    mtl_wishlist_price_drop_links,
    mtl_wishlist_price_drop_variant_sku
FROM 
    cart
    FULL OUTER JOIN wishlist ON cart.external_id=wishlist.external_id;
