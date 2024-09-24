INSERT INTO stg_curated.catalog_rental_plans_price_change_v1(
SELECT
	active,
	minimum_term_months,
	product_sku,
	rental_plan_price,
	product_id,
	version,
	published_at,
	created_at,
	updated_at,
	consumed_at::TIMESTAMP AS consumed_at,
	store_id,
	price_change_tag,
	id,
	price_change_reason,
	old_price,
	item_condition_id 
	FROM s3_spectrum_kafka_topics_raw.catalog_rental_plans_price_change_v1
WHERE consumed_at > (SELECT MAX(consumed_at) FROM stg_curated.catalog_rental_plans_price_change_v1)
);
