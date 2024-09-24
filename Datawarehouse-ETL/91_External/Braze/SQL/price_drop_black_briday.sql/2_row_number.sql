DROP TABLE IF EXISTS data_engineer.price_drop_r_num;
CREATE TEMP TABLE data_engineer.price_drop_r_num
AS
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
  ,updated_at
  ,ROW_NUMBER () OVER (PARTITION BY external_id ORDER BY updated_at DESC) AS rn
FROM 
  data_engineer.price_drop_enriched_historical
WHERE 
  updated_at IS NOT null;
