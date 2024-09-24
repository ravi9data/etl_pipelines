
drop table if exists trans_dev.pricing_finco_data;
create table trans_dev.pricing_finco_data as 
SELECT 
	reporting_date,
	product_sku,
	max(case when lower(srm."condition") = 'new' then final_price end) as new_price_finco,
	max(case when lower(srm."condition") = 'agan' then final_price end) as agan_price_finco,
	max(case when lower(srm."condition") = 'used' then final_price end) as used_price_finco,
	max(case when lower(srm."condition") = 'new' then m_since_last_valuation_price end) as months_since_last_valuation_new,
	max(case when lower(srm."condition") = 'agan' then m_since_last_valuation_price end) as months_since_last_valuation_agan,
	max(case when lower(srm."condition") = 'used' then m_since_last_valuation_price end) as months_since_last_valuation_used
FROM 
	ods_production.spv_report_master srm
where 
	reporting_date = (select DATE_TRUNC('month',max(reporting_date))-1 from ods_production.spv_report_master)
group by 1,2
;
