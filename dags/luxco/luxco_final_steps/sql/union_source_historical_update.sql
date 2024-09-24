-- IMP: Run  Only after final revision

insert into ods_spv_historical.union_sources_{{ params.tbl_suffix }} (
src,region,extract_date,reporting_month,item_id,product_sku,asset_condition,currency,price,price_in_euro)
select
	'MR' as src,
	case
		when capital_source in ('Grover Finance I GmbH') then 'Germany'
		when capital_source in ('USA_test') then 'USA'
		else null
	end as region,
	'{{ params.last_day_of_prev_month }}'::date as extract_date,
	'{{ params.date_for_depreciation }}'::date as reportmonth,
	99999999 as item_id,
	product_sku,
	asset_condition,
	case
		when capital_source in ('Grover Finance I GmbH') then 'EUR'
		when capital_source in ('USA_test') then 'USD'
		else null
	end,
	final_price as price,
	case
		when capital_source in ('Grover Finance I GmbH') then
		 final_price::DECIMAL(38,2)
		when capital_source in ('USA_test') then
		final_price*coalesce(exchange_rate_eur::DECIMAL(38,2),1)
	end as price_in_euro
from
	ods_spv_historical.luxco_manual_revisions  mr
	left join trans_dev.daily_exchange_rate er
		on '{{ params.last_day_of_prev_month }}' = er.date_
      where mr.luxco_month <= '{{ params.date_for_depreciation }}';

-- Push to union source

delete from ods_spv_historical.union_sources
where reporting_month = '{{ params.first_day_of_month }}';

insert into ods_spv_historical.union_sources(region,src,extract_date,reporting_month,item_id,product_sku,asset_condition,currency,price,price_in_euro)
select region,src,extract_date,reporting_month,item_id,product_sku,asset_condition,currency,price,price_in_euro
FROM
	ods_spv_historical.union_sources_{{ params.tbl_suffix }}
where reporting_month = '{{ params.first_day_of_month }}';
