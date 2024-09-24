DROP TABLE IF EXISTS staging_price_collection.ebay_us_copy_s3;
CREATE TABLE IF NOT EXISTS staging_price_collection.ebay_us_copy_s3
(
	item_id VARCHAR(65535)   ENCODE RAW
	,ebay_product_name VARCHAR(65535)   ENCODE RAW
	,price VARCHAR(65535)   ENCODE RAW
	,auction_type VARCHAR(65535)   ENCODE RAW
	,product_name VARCHAR(65535)   ENCODE RAW
	,brand VARCHAR(65535)   ENCODE RAW
	,product_sku VARCHAR(65535)   ENCODE RAW
	,ebay VARCHAR(65535)   ENCODE RAW
	,crawled_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	,condition VARCHAR(65535)   ENCODE RAW
)
DISTKEY(item_id)
SORTKEY(item_id);

CREATE TABLE IF NOT EXISTS staging_price_collection.ebay_us
(
	item_id VARCHAR(65535)   ENCODE RAW
	,ebay_product_name VARCHAR(65535)   ENCODE RAW
	,price VARCHAR(65535)   ENCODE RAW
	,auction_type VARCHAR(65535)   ENCODE RAW
	,product_name VARCHAR(65535)   ENCODE RAW
	,brand VARCHAR(65535)   ENCODE RAW
	,product_sku VARCHAR(65535)   ENCODE RAW
	,ebay VARCHAR(65535)   ENCODE RAW
	,crawled_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	,condition VARCHAR(65535)   ENCODE RAW
	,country VARCHAR(65535)   ENCODE RAW
	,inserted_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTKEY(item_id)
SORTKEY(item_id);
