drop table if exists dm_commercial.pricing_final_policy;
create table dm_commercial.pricing_final_policy as
SELECT
f.reporting_date,
f.product_sku,
p.category_name,
p.product_name,
f.ltd_avg_purchase_price,
f.months_since_last_purchase,
f.avg_price_mm_saturn,
f.pp_price,
f.new_price as new_price_median_coef_outlier_method,
f.new_vs_mm_sat as new_vs_mm_saturn_v1 ,
f.absolute_lowest_amazon_with_outlier_detection as absolute_lowest_amazon_with_outlier_detection_v1,
f.absolute_lowest_amazon_without_outlier_detection as absolute_lowest_amazon_without_outlier_detection_v1,
f.avg_3_lowest_price_with_outlier_detection as avg_3_lowest_price_with_outlier_detection_v1,
f.avg_3_lowest_price_without_outlier_detection as avg_3_lowest_price_without_outlier_detection_v1,
f2.new_price as new_price_deviation_ref_price_method,
f2.new_vs_mm_sat as new_vs_mm_saturn_v2 ,
f2.absolute_lowest_amazon_with_outlier_detection as absolute_lowest_amazon_with_outlier_detection_v2,
f2.absolute_lowest_amazon_without_outlier_detection as absolute_lowest_amazon_without_outlier_detection_v2,
f2.avg_3_lowest_price_with_outlier_detection as avg_3_lowest_price_with_outlier_detection_v2,
f2.avg_3_lowest_price_without_outlier_detection as avg_3_lowest_price_without_outlier_detection_v2,
f.finco_valuation_date,
f.new_price_finco,
f.months_since_last_valuation_new,
f.agan_price_finco,
f.months_since_last_valuation_agan,
f.used_price_finco,
f.months_since_last_valuation_used
from
trans_dev.pricing_final f
left join
trans_dev.pricing_final_2 f2
on f.product_sku= f2.product_sku
LEFT JOIN
	ods_production.product p
	ON p.product_sku= f.product_sku
;

drop table if exists trans_dev.pricing_outlier_removal;
drop table if exists trans_dev.pricing_outlier_removal_ranked;
drop table if exists  trans_dev.pricing_spv_used_asset_price_master;
drop table if exists trans_dev.pricing_finco_data;
drop table if exists trans_dev.master_sku_list;
drop table if exists trans_dev.pricing_final;
drop table if exists trans_dev.pricing_outlier_removal_2;
drop table if exists trans_dev.pricing_outlier_removal_ranked_2;
drop table if exists  trans_dev.pricing_spv_used_asset_price_master_2;
drop table if exists trans_dev.pricing_final_2;


drop table if exists skyvia.pricing_final_policy;
create table skyvia.pricing_final_policy as
select * from dm_commercial.pricing_final_policy;

GRANT SELECT ON ALL TABLES IN SCHEMA skyvia TO  skyvia;
