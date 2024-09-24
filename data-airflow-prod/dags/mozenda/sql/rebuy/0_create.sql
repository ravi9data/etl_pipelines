DROP TABLE IF EXISTS staging_price_collection.rebuy_copy_s3;
CREATE TABLE IF NOT EXISTS staging_price_collection.rebuy_copy_s3
(

	item_id VARCHAR(200)   ENCODE RAW
	,condition VARCHAR(200)   ENCODE RAW
	,price VARCHAR(200)   ENCODE RAW
	,variant_sku VARCHAR(200)   ENCODE RAW
	,full_ean VARCHAR(200)   ENCODE RAW
	,brand VARCHAR(200)   ENCODE RAW
	,product_name VARCHAR(3000)   ENCODE RAW
	,product_sku VARCHAR(200)   ENCODE RAW
	,name VARCHAR(200)   ENCODE RAW
	,crawled_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTKEY(item_id)
SORTKEY(item_id);

CREATE TABLE IF NOT EXISTS staging_price_collection.rebuy
(
	item_id VARCHAR(200)   ENCODE RAW
	,condition VARCHAR(200)   ENCODE RAW
	,price VARCHAR(200)   ENCODE RAW
	,variant_sku VARCHAR(200)   ENCODE RAW
	,full_ean VARCHAR(200)   ENCODE RAW
	,brand VARCHAR(200)   ENCODE RAW
	,product_name VARCHAR(3000)   ENCODE RAW
	,product_sku VARCHAR(200)   ENCODE RAW
	,name VARCHAR(200)   ENCODE RAW
	,crawled_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	,inserted_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTKEY(item_id)
SORTKEY(item_id);
