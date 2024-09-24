

DROP TABLE IF EXISTS tmp_bi_ods_product;
CREATE TEMP TABLE tmp_bi_ods_product AS 

with category as (
select distinct
c.category_id as category_id,
c.category_name as category_name,
s.id as subcategory_id,
s.name as subcategory_name
from stg_api_production.categories s
left join (
select distinct
 id as category_id,
 parent_id as parent_id,
 name as category_name
from stg_api_production.categories
where parent_id is null) c  on s.parent_id = c.category_id
where c.category_id is not NULL
)
select distinct
p.id as product_id,
sku as product_sku,
p.created_at,
updated_at,
name as product_name,
c.category_id,
c.subcategory_id,
c.category_name,
c.subcategory_name,
upper(p.brand) as brand,
slug,
p.market_price,
p.rank,
rl.risk_label,
rl.risk_category
--p.availability_state
from stg_api_production.spree_products p
inner join category c on p.category_id=c.subcategory_id
left join stg_fraud_and_credit_risk.subcategory_brand_risk_labels rl
on (lower(c.subcategory_name) = rl.subcategory  and lower(p.brand) = rl.brand)
where p.id != 9251
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