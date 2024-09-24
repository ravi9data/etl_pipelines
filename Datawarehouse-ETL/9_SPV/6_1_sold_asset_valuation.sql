drop table if exists ods_spv_historical.sold_asset_valuation;
create table ods_spv_historical.sold_asset_valuation as 
with spv_prep as (
 select distinct spv.* 
 from ods_production.spv_report_master spv 
 left join public.dim_dates dd 
  on spv.reporting_date::date=dd.datum
 where final_price >0
  and ((dd.day_is_last_of_month) or (spv.reporting_date::date <='2019-12-01' and dd.day_is_first_of_month))
 )
select distinct 
asset_id, 
first_value(final_price) 
 over (partition by asset_id order by reporting_date desc
 rows unbounded preceding) as last_final_valuation,
first_value(valuation_1) 
 over (partition by asset_id order by reporting_date desc
 rows unbounded preceding) as last_valuation_1,
first_value(valuation_2) 
 over (partition by asset_id order by reporting_date desc
 rows unbounded preceding) as last_valuation_2,
 first_value(valuation_3) 
 over (partition by asset_id order by reporting_date desc
 rows unbounded preceding) as last_valuation_3,
first_value(reporting_date) 
 over (partition by asset_id order by reporting_date desc
 rows unbounded preceding) as last_valuation_report_date
from spv_prep
;