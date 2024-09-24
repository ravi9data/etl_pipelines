DROP TABLE IF EXISTS tmp_bi_ods_product;
CREATE TEMP TABLE tmp_bi_ods_product AS 

WITH category AS (
SELECT DISTINCT
c.category_id AS category_id,
c.category_name AS category_name,
s.id AS subcategory_id,
s.name AS subcategory_name
FROM stg_api_production.categories s
LEFT JOIN (
SELECT DISTINCT
 id AS category_id,
 parent_id AS parent_id,
 name AS category_name
FROM stg_api_production.categories
WHERE parent_id IS NULL) c  ON s.parent_id = c.category_id
WHERE c.category_id IS NOT NULL
)
SELECT DISTINCT
p.id AS product_id,
sku AS product_sku,
p.created_at,
updated_at,
name AS product_name,
c.category_id,
c.subcategory_id,
c.category_name,
c.subcategory_name,
UPPER(p.brand) AS brand,
slug,
p.market_price,
p.rank,
rl.risk_label,
rl.risk_category
--p.availability_state
FROM stg_api_production.spree_products p
INNER JOIN category c ON p.category_id=c.subcategory_id
LEFT JOIN stg_fraud_and_credit_risk.subcategory_brand_risk_labels rl
ON (LOWER(c.subcategory_name) = rl.subcategory  AND LOWER(p.brand) = rl.brand)
WHERE p.id != 9251
;

BEGIN TRANSACTION;

DELETE FROM bi_ods.product WHERE 1=1;

INSERT INTO bi_ods.product
SELECT * FROM tmp_bi_ods_product;

END TRANSACTION;

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null