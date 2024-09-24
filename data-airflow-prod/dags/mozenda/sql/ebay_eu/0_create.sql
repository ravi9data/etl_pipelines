DROP TABLE IF EXISTS staging_price_collection.ebay_eu_copy_s3;
CREATE TABLE IF NOT EXISTS staging_price_collection.ebay_eu_copy_s3
(
	item_id VARCHAR(200)   ENCODE RAW
	,ebay_product_name VARCHAR(200)   ENCODE RAW
	,price VARCHAR(200)   ENCODE RAW
	,auction_type VARCHAR(200)   ENCODE RAW
	,product_name VARCHAR(65535)   ENCODE RAW
	,brand VARCHAR(65535)   ENCODE RAW
	,product_sku VARCHAR(200)   ENCODE RAW
	,ebay VARCHAR(200)   ENCODE RAW
	,crawled_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	,condition VARCHAR(200)   ENCODE RAW
)
DISTKEY(item_id)
SORTKEY(item_id);

CREATE TABLE IF NOT EXISTS staging_price_collection.ebay_eu
(
	item_id VARCHAR(200)   ENCODE RAW
	,ebay_product_name VARCHAR(200)   ENCODE RAW
	,price VARCHAR(200)   ENCODE RAW
	,auction_type VARCHAR(200)   ENCODE RAW
	,product_name VARCHAR(65535)   ENCODE RAW
	,brand VARCHAR(65535)   ENCODE RAW
	,product_sku VARCHAR(200)   ENCODE RAW
	,ebay VARCHAR(200)   ENCODE RAW
	,crawled_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	,condition VARCHAR(200)   ENCODE RAW
	,country VARCHAR(2)   ENCODE RAW	
	,inserted_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTKEY(item_id)
SORTKEY(item_id);
