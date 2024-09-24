insert into ods_production.product
select *
from ods_production.stg_product
where product_id not in (select product_id from ods_production.product);
