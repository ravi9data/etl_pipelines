CREATE TABLE IF NOT EXISTS data_engineer.price_drop_enriched_historical
(
	external_id INTEGER   ENCODE RAW
	,mtl_cart_price_drop_names VARCHAR(65535)   ENCODE lzo
	,mtl_cart_price_drop_full_prices VARCHAR(65535)   ENCODE lzo
	,mtl_cart_price_drop_sale_prices VARCHAR(65535)   ENCODE lzo
	,mtl_cart_price_drop_images VARCHAR(65535)   ENCODE lzo
	,mtl_cart_price_drop_links VARCHAR(65535)   ENCODE lzo
	,mtl_wishlist_price_drop_names VARCHAR(65535)   ENCODE lzo
	,mtl_wishlist_price_drop_full_prices VARCHAR(65535)   ENCODE lzo
	,mtl_wishlist_price_drop_sale_prices VARCHAR(65535)   ENCODE lzo
	,mtl_wishlist_price_drop_images VARCHAR(65535)   ENCODE lzo
	,mtl_wishlist_price_drop_links VARCHAR(65535)   ENCODE lzo
	,updated_at TIMESTAMP ENCODE RAW
)
DISTSTYLE KEY
 DISTKEY (external_id)
 SORTKEY (external_id);
