BEGIN;

DELETE
FROM
		staging_price_collection.amazon
		USING staging_price_collection.amazon_copy_s3
WHERE
	amazon.item_id = amazon_copy_s3.item_id;

INSERT INTO staging_price_collection.amazon
SELECT
	item_id,
	product_name,
	brand,
	product_sku,
	amazon_name,
	main_ad_condition,
	main_ad_price,
	condition,
	price,
	crawled_at,
	CURRENT_TIMESTAMP AS inserted_at
FROM
	staging_price_collection.amazon_copy_s3;

COMMIT;
