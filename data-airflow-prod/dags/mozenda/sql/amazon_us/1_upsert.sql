BEGIN;

DELETE
FROM
		staging_price_collection.amazon_us
		USING staging_price_collection.amazon_us_copy_s3
WHERE
	amazon_us.item_id = amazon_us_copy_s3.item_id;

INSERT INTO staging_price_collection.amazon_us
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
	'US' as country,
	crawled_at,
	CURRENT_TIMESTAMP AS inserted_at
FROM
	staging_price_collection.amazon_us_copy_s3;

COMMIT;
