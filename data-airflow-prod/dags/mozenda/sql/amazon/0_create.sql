DROP TABLE IF EXISTS staging_price_collection.amazon_copy_s3;
CREATE TABLE IF NOT EXISTS staging_price_collection.amazon_copy_s3
(
	item_id VARCHAR(200)   ENCODE RAW
	,product_name VARCHAR(2000)   ENCODE RAW
	,brand VARCHAR(65000)   ENCODE RAW
	,product_sku VARCHAR(2000)   ENCODE RAW
	,amazon_name VARCHAR(2000)   ENCODE RAW
	,main_ad_condition VARCHAR(2000)   ENCODE RAW
	,main_ad_price VARCHAR(2000)   ENCODE RAW
	,condition VARCHAR(2000)   ENCODE RAW
	,price VARCHAR(2000)   ENCODE RAW
	,crawled_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTKEY(item_id)
SORTKEY(item_id);

CREATE TABLE IF NOT EXISTS staging_price_collection.amazon
(
	item_id VARCHAR(200)   ENCODE RAW
	,product_name VARCHAR(2000)   ENCODE RAW
	,brand VARCHAR(65000)   ENCODE RAW
	,product_sku VARCHAR(2000)   ENCODE RAW
	,amazon_name VARCHAR(2000)   ENCODE RAW
	,main_ad_condition VARCHAR(2000)   ENCODE RAW
	,main_ad_price VARCHAR(2000)   ENCODE RAW
	,condition VARCHAR(2000)   ENCODE RAW
	,price VARCHAR(2000)   ENCODE RAW
	,crawled_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	,inserted_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTKEY(item_id)
SORTKEY(item_id);
