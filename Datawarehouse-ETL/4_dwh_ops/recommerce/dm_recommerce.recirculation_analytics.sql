drop table if exists dm_recommerce.recirculation_analytics;
create table dm_recommerce.recirculation_analytics as

with asset_subscription_revenue_before_date_processing as(
select * from(
select lrm.asset_id , 
lrm.serial_number, 
lrm.subscription_revenue_paid_lifetime as subscription_revenue_before_date_processing, 
lrm.reporting_date, 
skf.date_of_processing,
skf.channeling,
skf.mapping,
row_number() over(partition by lrm.serial_number order by lrm.reporting_date desc) as rn
from dm_recommerce.spl_kd_final skf 
left join dm_finance.luxco_report_master lrm  on lrm.serial_number = skf.serialnumber and lrm.reporting_date::date<skf.date_of_processing::date )
where rn=1
),

asset_subscription_revenue_after_date_processing as(
select * from(
select lrm.asset_id , 
lrm.serial_number, 
lrm.subscription_revenue_paid_lifetime as subscription_revenue_after_date_processing, 
lrm.reporting_date, 
skf.date_of_processing,
skf.channeling,
skf.mapping,
row_number() over(partition by lrm.serial_number order by lrm.reporting_date desc) as rn
from dm_recommerce.spl_kd_final skf 
left join dm_finance.luxco_report_master lrm  on lrm.serial_number = skf.serialnumber and lrm.reporting_date::date>=skf.date_of_processing::date )
where rn=1 
),

subscription_payment as(
select s.subscription_id,
a2.serial_number,
max(s.paid_date) as date_of_last_payment
from ods_production.payment_subscription s
left join master.allocation a on a.subscription_id = s.subscription_id
left join master.asset a2 on a2.asset_id = a.asset_id 
group by 1,2
),


subscription_detail as(
select * from (
select 
a.serial_number,
ah.subscription_id as last_active_subscription_id,
s.start_date as date_of_last_active_subscription ,
s.subscription_plan as last_active_subscription_plan,
s.subscription_value as last_active_subscription_value,
s.minimum_cancellation_date,
ah.date,
skf.date_of_processing, 
skf.channeling, 
skf.mapping,
row_number() over(partition by a.asset_id order by ah."date" desc) as rn
from dm_recommerce.spl_kd_final skf 
left join master.asset a on a.serial_number = skf.serialnumber 
left join master.allocation_historical ah on ah.asset_id = a.asset_id and ah."date"::date>skf.date_of_processing::date 
left join master.subscription s on ah.subscription_id = s.subscription_id where s.status='ACTIVE') 
where rn=1
),

repair_estimated_cost as(
select 
rce.serial_number, 
rce.price_netto,
rce.price_brutto,
rce.repair_partner 
from dm_recommerce.repair_cost_estimates rce
),

repair_cost as(
select ri.serial_number,
ri.repair_price,
ri.repair_partner
from dm_recommerce.repair_invoices ri

)


select a.asset_id
,rb.subscription_revenue_before_date_processing
,ra.subscription_revenue_after_date_processing
,skf.date_of_processing
,skf.serialnumber as serial_number
,a.category_name
,a.subcategory_name
,a.brand
,a.asset_status_original
,a.initial_price
,a.residual_value_market_price
,sd.last_active_subscription_id 
,sd.date_of_last_active_subscription
,sd.last_active_subscription_plan
,sd.last_active_subscription_value
,sd.minimum_cancellation_date
,rec.price_netto as estimated_repair_price_net
,rec.price_brutto as estimated_repair_price_gross
,rc.repair_partner
,rc.repair_price
,skf.channeling 
,skf.mapping
,case when a.asset_status_detailed='SOLD to 3rd party' then a.sold_price end as sold_price
,case when a.asset_status_detailed='SOLD to 3rd party' then a.sold_date  end as sold_date
from dm_recommerce.spl_kd_final skf
left join asset_subscription_revenue_before_date_processing rb on skf.serialnumber = rb.serial_number
left join asset_subscription_revenue_after_date_processing ra on ra.serial_number =skf.serialnumber
left join subscription_detail sd on sd.serial_number = skf.serialnumber
left join repair_estimated_cost rec on rec.serial_number= skf.serialnumber
left join repair_cost rc on rc.serial_number = skf.serialnumber
left join master.asset a on a.serial_number = skf.serialnumber;

GRANT SELECT ON dm_recommerce.recirculation_analytics TO tableau;
