truncate dm_recommerce.recommerce_stages_financial;

insert into dm_recommerce.recommerce_stages_financial
with in_process_assets as(
select fact_date,
serial_number,
asset_id
from(
select *,
row_number() over(partition by serial_number, date_trunc('month', fact_date) order by fact_date desc) as rn
from dm_recommerce.recommerce_stages 
where  kpi='In Process')as sub_q where rn=1
),

value_in_process as(
select pa.fact_date,
pa.serial_number,
'Asset Value in Process' as kpi,
dr.residual_value as value
from in_process_assets pa
left join dm_finance.depreciation_monthend_report dr on pa.asset_id = dr.asset_id
and (case when date_trunc('month',pa.fact_date)=date_trunc('month', current_date)
then date_add('month',-1,last_day(pa.fact_date::date)) else last_day(pa.fact_date::date) end)::date=dr.report_date::date
),

processed_assets as(
select 
i.asset_id,
i.asset_name,
i.brand,
i.category_name,
i.channel,
i.initial_price,
i.invoice_type,
i.partner,
i.partner_id,
i.sales_price_gross,
i.sales_price_net, --Revenue
i.total_billing, --Cost
i.serial_number,
i.sold_date,
i.subcategory_name,
ah2.residual_value as residual_value_on_sold_date --Write-off Value
from dm_recommerce.recommerce_b2b_b2c_invoices i
left join dm_finance.depreciation_monthend_report ah2 on ah2.asset_id = i.asset_id
and ah2.report_date = last_day(date_add('month',-1,i.sold_date))),

revenue as(
select sold_date as fact_date,
serial_number,
'Revenue' as kpi,
sales_price_net as value
from processed_assets
),

cost as(
select sold_date as fact_date,
serial_number,
'Cost' as kpi,
total_billing as value
from processed_assets
),

write_off_value as(
select sold_date as fact_date,
serial_number,
'Write-off Value' as kpi,
residual_value_on_sold_date as value
from processed_assets
),

profit as(
select sold_date as fact_date,
'Profit' as kpi,
sum(sales_price_net)-sum(total_billing) as value
from processed_assets
group by 1,2
)

select * from value_in_process
union all
select * from revenue
union all
select * from cost
union all
select * from write_off_value
union all
select fact_date,
'' as serial_number,
kpi,
value
from profit;
