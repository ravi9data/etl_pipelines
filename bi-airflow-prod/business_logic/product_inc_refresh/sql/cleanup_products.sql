delete from ods_production.product
using ods_production.stg_product p2
where p2.product_id = product.product_id
and p2.check_sum <> product.check_sum;
