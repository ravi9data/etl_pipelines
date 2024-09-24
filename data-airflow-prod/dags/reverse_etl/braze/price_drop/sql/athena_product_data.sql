WITH latest_old_price as (
SELECT
    rental_plan_id,
    price
FROM
    "old_prices"
WHERE
    price IS NOT null),

prep_data AS (
SELECT
    spree_products.id AS product_id,
    spree_products.name AS product_name,
    spree_products.slug AS product_slug,
    spree_products.sku AS product_sku,
    rental_plans.id AS rental_plan_id,
    rental_plans.active AS rental_plan_active,
    spree_variants.id AS variant_id,
    variant_stores.availability,
    variant_stores.store_id,
    rental_plans.rental_plan_price AS current_price,
    latest_old_price.price AS old_price,
    cast(latest_old_price.price AS DOUBLE) - cast(rental_plans.rental_plan_price AS DOUBLE) AS price_diff,
    REPLACE(image, :image_path;, :emptystring;) AS image_url,
    ROW_NUMBER() OVER(PARTITION BY store.id, spree_products.id
                          ORDER BY CAST(pics.id as INT), CAST(rental_plans.rental_plan_price AS DOUBLE) ASC) AS r_num
FROM "spree_products"
LEFT JOIN "rental_plans" ON spree_products.id = rental_plans.product_id and rental_plans.active='True'
LEFT JOIN "spree_variants" ON spree_products.id = spree_variants.product_id and spree_variants.deprecated = 'False' and spree_variants.deleted_at IS NULL
LEFT JOIN "variant_stores" ON  spree_variants.id = variant_stores.variant_id and variant_stores.store_id=rental_plans.store_id
LEFT JOIN "latest_old_price" ON latest_old_price.rental_plan_id=rental_plans.id
LEFT JOIN "pictures" pics ON pics.variant_id=variant_stores.variant_id
JOIN "spree_stores" AS store ON store.id=variant_stores.store_id
    AND store_type=:store_type;
    AND code IN (:code;)   
where
    variant_stores.availability = :availability;
    AND latest_old_price.price is not null
    AND (cast(latest_old_price.price AS DOUBLE) - cast(rental_plans.rental_plan_price AS DOUBLE) > 0)
)

SELECT 
    product_id,
    product_name,
    product_slug,
    product_sku,
    rental_plan_id,
    rental_plan_active,
    variant_id,
    availability,
    store_id,
    current_price,
    old_price,
    price_diff,
    image_url,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_at
FROM
    prep_data
where
    r_num=1;
