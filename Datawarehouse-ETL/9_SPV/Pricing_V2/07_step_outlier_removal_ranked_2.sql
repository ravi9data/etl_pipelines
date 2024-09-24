
drop table if exists trans_dev.pricing_outlier_removal_ranked_2;
create table trans_dev.pricing_outlier_removal_ranked_2 as 
select
*,
rank() over(partition by product_sku, extract_date order by price) as price_rank_reordered
from 
trans_dev.pricing_outlier_removal_2
where src ='AMAZON'
;
