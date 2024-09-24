BEGIN;

DELETE
FROM
	staging_price_collection.ebay_us
	USING staging_price_collection.ebay_us_copy_s3
WHERE
	ebay_us.item_id = ebay_us_copy_s3.item_id;

INSERT INTO staging_price_collection.ebay_us
SELECT
	item_id
	,ebay_product_name
	,price
	,auction_type
	,product_name
	,brand
	,product_sku
	,ebay
	,crawled_at
	,condition
	,'US' AS country
	,CURRENT_TIMESTAMP AS inserted_at
FROM
	staging_price_collection.ebay_us_copy_s3;

COMMIT;
