drop table if exists dm_sustainability.devices
create table dm_sustainability.devices as
with dates as 
        (
        select datum from public.dim_dates
        where day_is_last_of_month 
        and datum >= '2021-12-31'
        and datum < current_date 
       )
       , allo as (
       select * from 
       master.allocation_historical ah
       inner join dates d on ah."date" = d.datum 
        )
        , 
        asset as
         (
       select * from 
       master.asset_historical ah2  
       inner join dates d on ah2."date" = d.datum 
        )
        , 
        sub as
         (
       select * from 
       master.subscription_historical s  
       inner join dates d on s."date" = d.datum 
        )
        ,a as (
	select 
		ast.asset_id,
date_part('year',purchased_date) as purchase_cohort,
ast."date" as report_date,
subcategory_name,
category_name,
asset_status_original,
asset_status_detailed,
count(*) as number_of_assets,
sum(coalesce(delivered_allocations,0)) as number_of_rental_cycles,
avg(coalesce(delivered_allocations,0)::decimal(10,2))::decimal(10,2) as avg_number_of_cycles,
sum(coalesce(effective_duration,0)) as total_rental_period,
avg(coalesce(effective_duration,0)::decimal(10,2))::decimal(10,2) as avg_rental_period_asset,
case when number_of_rental_cycles = 0 then 0 else (total_rental_period/number_of_rental_cycles) end as avg_rental_subscription_period,
sum(coalesce(days_on_rent,0)) as total_days_on_rent,
avg(case when asset_status_new = 'SOLD' then months_on_book::decimal(10,2) end)::decimal(10,2) as age_when_sold,
count(case when asset_status_detailed in ('SOLD 1-euro','SOLD to Customer') then ast.asset_id end) as asset_sold_to_customer,
count(case when asset_status_detailed in ('SOLD to 3rd party') then ast.asset_id end) as asset_sold_to_third_party,
avg(months_on_book::decimal(10,2))::decimal(10,2) as avg_product_age
from asset ast
left join 
(select asset_id,
		a."date",
	sum(case when delivered_at is not null then 
			case when rank_allocations_per_subscription = 1
					then
							(coalesce(cancellation_returned_at::date,s.cancellation_date::date,return_delivery_date::date,a."date")	-((delivered_at::date)))
					else 
							(coalesce(cancellation_returned_at::date,s.cancellation_date::date,return_delivery_date::date,a."date")	-((delivered_at::date))) end			
				else 0 end) as days_on_rent,
				sum(s.effective_duration)	as effective_duration				
from allo a 
left join sub s on a.subscription_id = s.subscription_id and a."date" = s."date"
--where a."date" = '2021-12-31'
group by 1,2) al on al.asset_id = ast.asset_id  and al."date" = ast."date"
--where ast."date" = '2021-12-31'
group by 1,2,3,4,5,6,7
order by 2 desc )
select * from a;