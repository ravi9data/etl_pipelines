INSERT INTO data_engineer.price_drop_enriched_historical 
(
SELECT 
  external_id
  ,mtl_cart_price_drop_names
  ,mtl_cart_price_drop_full_prices
  ,mtl_cart_price_drop_sale_prices
  ,mtl_cart_price_drop_images
  ,mtl_cart_price_drop_links
  ,mtl_wishlist_price_drop_names
  ,mtl_wishlist_price_drop_full_prices
  ,mtl_wishlist_price_drop_sale_prices
  ,mtl_wishlist_price_drop_images
  ,mtl_wishlist_price_drop_links
  ,CURRENT_TIMESTAMP AS updated_at
FROM 
	data_engineer.price_drop_enriched);
