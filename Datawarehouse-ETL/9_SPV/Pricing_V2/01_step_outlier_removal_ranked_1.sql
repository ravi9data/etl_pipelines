
drop table if exists trans_dev.pricing_outlier_removal_ranked;
create table trans_dev.pricing_outlier_removal_ranked as 
select
*,
rank() over(partition by product_sku, extract_date order by price) as price_rank_reordered
from 
trans_dev.pricing_outlier_removal
where src ='AMAZON'
;

