BEGIN;

DELETE
FROM
	staging_price_collection.rebuy
	USING staging_price_collection.rebuy_copy_s3
WHERE
	rebuy.item_id = rebuy_copy_s3.item_id;

INSERT INTO staging_price_collection.rebuy
SELECT
	item_id
	,condition
	,price
	,variant_sku
	,full_ean
	,brand
	,product_name
	,product_sku
	,name
	,crawled_at
	,CURRENT_TIMESTAMP AS inserted_at
FROM
	staging_price_collection.rebuy_copy_s3;

COMMIT;
