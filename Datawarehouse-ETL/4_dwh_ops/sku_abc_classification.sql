DROP TABLE IF EXISTS dm_operations.sku_abc_classification;
CREATE TABLE dm_operations.sku_abc_classification as
with ups_sku as (
	select 
		sku as variant_sku ,
		count(case when is_latest_report then serial_number end) total_inventory
	from dm_operations.ups_eu_reconciliation 
	where report_date > current_date - 180
	and asset_id is not null
	group by 1
),
---
--- 
--- Calculate Each Metric
---
total_revenue as (
	select s.variant_sku , sum(amount_paid) / 1000 total_revenue 
	from master.subscription_payment p 
	left join master.subscription s on p.subscription_id  = s.subscription_id  
	where paid_date > current_date - 365
	and s.variant_sku in (select variant_sku from ups_sku)
	group by 1
),
total_allocations as (
	select t.variant_sku  , count(1) total_allocations 
	from master.allocation a
	left join master.asset t using (asset_id)
	where shipment_at > current_date - 365
	and t.variant_sku in (select variant_sku from ups_sku)
	and warehouse = 'ups_softeon_eu_nlrng'
	group by 1
),
asset_value as (
	select distinct 
		variant_sku ,
		--redshift does not calculate multiple medians at once
		median(initial_price) over (partition by variant_sku ) purchase_price,
		median(residual_value_market_price)  over (partition by variant_sku ) residual_value 
	from master.asset
	where variant_sku in (select variant_sku from ups_sku)
),
days_in_stock as (
	select 
		variant_sku ,
		AVG(case when asset_status_original = 'IN STOCK' then coalesce(days_in_stock,0) end ) days_in_stock 
	from ods_production.asset 
	where variant_sku in (select variant_sku from ups_sku)
	group by 1
),
---
--- 
--- Join them all , Variant SKU
---
joined as (
select 
	u.variant_sku ,
	v.variant_name as asset_name , --using asset name is not a good idea, because changes do not reflect
	u.total_inventory,
	round(coalesce (tr.total_revenue, 0), 0) total_revenue ,
	coalesce (ta.total_allocations, 0) total_allocations ,
	round(coalesce (av.purchase_price, 0)) purchase_price ,
	round(coalesce (av.residual_value, 0)) residual_value ,
	coalesce (dis.days_in_stock, 0) days_in_stock,
	(dm.final_height * dm.final_width * dm.final_length) as volume
from ups_sku u 
left join ods_production.variant v using (variant_sku)
left join total_revenue tr using (variant_sku)
left join total_allocations ta using (variant_sku)
left join asset_value av using (variant_sku)
left join days_in_stock dis using (variant_sku)
left join dm_operations.dimensions dm using (variant_sku)
),
---
--- 
--- Find Min Max to normalize values in next ste
--- Multiple min-max operations not supported
---
find_minmax as (
select 
	min(total_revenue) as min_total_revenue,
	max(total_revenue) - min_total_revenue as delta_total_revenue,
	min(total_allocations) as min_total_allocations,
	max(total_allocations) - min_total_allocations as delta_total_allocations,
	min(purchase_price) as min_purchase_price,
	max(purchase_price) - min_purchase_price as delta_purchase_price,
	min(residual_value) as min_residual_value,
	max(residual_value) - min_residual_value as delta_residual_value,
	min(days_in_stock) as min_days_in_stock,
	max(days_in_stock) - min_days_in_stock as delta_days_in_stock,
	min(volume) as min_volume,
	max(volume) - min_volume as delta_volume
from joined 
),
---
--- 1. Normalized Value = (Value - Min) / (Max - Min) 
--- 2. Total Score = Coefficient * Normalized Value
--- 3. Final Score = All Total Scores
---
normalize_all as ( 
	select 
		j.variant_sku,
		j.asset_name,
		j.total_inventory,
		j.total_revenue ,
		j.total_allocations ,
		j.purchase_price ,
		j.residual_value ,
		j.days_in_stock ,
		j.volume ,
		(j.total_revenue - m.min_total_revenue) / m.delta_total_revenue as n_total_revenue,
		(j.total_allocations - m.min_total_allocations)::float / m.delta_total_allocations::float as n_total_allocations,
		(j.purchase_price - m.min_purchase_price) / m.delta_purchase_price as n_purchase_price,
		(j.residual_value - m.min_residual_value) / m.delta_residual_value as n_residual_value,
		-- Days In Stock , this metric has negative impact
		-- That is why it is substracted from 100
		1 - (j.days_in_stock - m.min_days_in_stock)::float / m.delta_days_in_stock::float as n_days_in_stock,
		1 - (j.volume - m.min_volume) / m.delta_volume as n_volume,
		-- final score
		round(
			coalesce (
			100 * n_total_revenue + 
			80 * n_total_allocations + 
			80 * n_purchase_price + 
			40 * n_residual_value + 
			10 * n_days_in_stock +
			5  * n_volume,
			0), 1)
		as final_score
	from joined j
	cross join find_minmax m  
)
select 
	variant_sku ,
	asset_name ,
	total_inventory,
	final_score ,
	round (	percent_rank() over (order by final_score) * 100, 1) as percentrank,
	case when percentrank > 90 then 'A'
		 when percentrank > 20 then 'B'
		 else 'C' end abc_class,
	total_revenue ,
	total_allocations ,
	purchase_price ,
	residual_value ,
	days_in_stock ,
	volume 
from normalize_all ;

GRANT SELECT ON dm_operations.sku_abc_classification TO tableau;