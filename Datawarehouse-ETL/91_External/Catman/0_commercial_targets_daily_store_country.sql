-- ASV targets for 2020

drop table if exists dwh.commercial_targets_daily_store_country_2020;
create table dwh.commercial_targets_daily_store_country_2020 as
with a as (
select 
country, store,
store||'-'||country as store_country,
last_day(bom)::date AS eom,
bom,
(last_day(bom)::date-bom::date)+1 as days_in_month,
coalesce(lag(activesubsvalue) over (partition by country, store order by eom)
 ,activesubsvalue-incrementalsubsvalue) as activesubscriptionvalue_bom,
activesubsvalue as activesubscriptionvalue_eom,
coalesce(activesubsvalue-activesubscriptionvalue_bom,incrementalsubsvalue) as monthly_growth,
case when incrementalsubsvalue='' then null else incrementalsubsvalue end as incrementalsubsvalue,
(case when cancelledsubvalue='' then null else cancelledsubvalue end)::numeric(29,11) as cancelledsubvalue,
(case when acquiredsubsvalue='' then null else acquiredsubsvalue end)::numeric(29,11) as acquiredsubsvalue,
(case when acquiredsubs='' then null else acquiredsubs end)::numeric(29,11) as acquiredsubs,
(case when subsperorder='' then null else subsperorder end)::numeric(29,11) as subsperorder,
case when paidorders='' then null else paidorders end as paidorders,
case when paidrate='' then null else paidrate end as paidrate,
case when approvedorders='' then null else approvedorders end as approvedorders,
case when approvalrate='' then null else approvalrate end as approvalrate,
case when marketingcostvouchers='' then null else marketingcostvouchers end as marketingcostvouchers
from stg_external_apis.gs_commercial_targets_2020 t 
order by 1)
--
  select 
      datum , 
      day_name,
    a.*,
      (activesubscriptionvalue_eom-activesubscriptionvalue_bom)/days_in_month as daily_growth_linear,
      sum((activesubscriptionvalue_eom-activesubscriptionvalue_bom)/days_in_month) 
        over (PARTITION by bom,country, store order by datum rows unbounded preceding) as cumulative_sum,
      (sum((activesubscriptionvalue_eom-activesubscriptionvalue_bom)/days_in_month) 
        over (PARTITION by bom,country, store order by datum rows unbounded preceding))+activesubscriptionvalue_bom as active_sub_value_daily_target,
    marketingcostvouchers/days_in_month as marketingcostvouchers_daily,
    incrementalsubsvalue/days_in_month as incrementalsubsvalue_daily,
  cancelledsubvalue/days_in_month as cancelled_subvalue_daily,
  acquiredsubsvalue/days_in_month as acquired_subvalue_daily,
  acquiredsubs/days_in_month as acquied_subs_daily,
  subsperorder/days_in_month as subsperorder_daily,
  paidorders/days_in_month as paidorders_daily,
  paidrate as paid_rate,
  approvedorders/days_in_month as approvedorders_daily,
  approvalrate as approval_rate
   from  a 
   left join public.dim_dates
   on a.bom=date_trunc('month',datum::date)::date
   where datum>='2019-02-01'
   order by 1 desc;
   
 
  -- ASV targets for 2021

drop table if exists dwh.commercial_targets_daily_store_country_2021;
create table dwh.commercial_targets_daily_store_country_2021 as
with targets2021 as (
    select
        trunc(datum) as datum,
        to_char(datum::date, 'Day') as day_name,
        country,
        store,
        store||'-'||country as store_country,
        last_day(datum)::date AS eom,
        date_trunc('month', datum) as bom,
        substring(last_day(datum),9,2)::integer as days_in_month,
        activesubsvalue,
        first_value(activesubsvalue) over (partition by country,store, date_trunc('month', datum) order by datum rows between unbounded preceding and unbounded following) as activesubscriptionvalue_bom,
        last_value(activesubsvalue) over (partition by country,store, date_trunc('month', datum) order by datum rows between unbounded preceding and unbounded following) as activesubscriptionvalue_eom,
        coalesce(activesubsvalue - activesubscriptionvalue_bom,incrementalsubsvalue) as monthly_growth,
        case when incrementalsubsvalue='' then null else incrementalsubsvalue end as incrementalsubsvalue,
        case when cancelledsubvalue='' then null else cancelledsubvalue end as cancelledsubvalue,
        case when acquiredsubsvalue='' then null else acquiredsubsvalue end as acquiredsubsvalue,
        case when acquiredsubs='' then null else acquiredsubs end as acquiredsubs,
        case when subsperorder='' then null else subsperorder end as subsperorder,
        case when paidorders='' then null else paidorders end as paidorders,
        case when paidrate='' then null else paidrate end as paidrate,
        case when approvedorders='' then null else approvedorders end as approvedorders,
        case when approvalrate='' then null else approvalrate end as approvalrate,
        case when marketingcostvouchers='' then null else marketingcostvouchers::double precision end as marketingcostvouchers
    from stg_external_apis.gs_commercial_targets_2021 where trunc(datum)<'2021-09-01'
    union
    select
        trunc(datum) as datum,
        to_char(datum::date, 'Day') as day_name,
        country,
        store,
        store||'-'||country as store_country,
        last_day(datum)::date AS eom,
        date_trunc('month', datum) as bom,
        substring(last_day(datum),9,2)::integer as days_in_month,
        active_subs_value,
        first_value(active_subs_value) over (partition by country,store, date_trunc('month', datum) order by datum rows between unbounded preceding and unbounded following) as activesubscriptionvalue_bom,
        last_value(active_subs_value) over (partition by country,store, date_trunc('month', datum) order by datum rows between unbounded preceding and unbounded following) as activesubscriptionvalue_eom,
        coalesce(active_subs_value - activesubscriptionvalue_bom,incremental_subs_value) as monthly_growth,
        case when incremental_subs_value='' then null else incremental_subs_value end as incrementalsubsvalue,
        case when cancelled_sub_value='' then null else cancelled_sub_value end as cancelledsubvalue,
        case when acquired_subs_value='' then null else acquired_subs_value end as acquiredsubsvalue,
        case when acquired_subs='' then null else acquired_subs end as acquiredsubs,
        case when subs_per_order='' then null else subs_per_order end as subsperorder,
        case when paid_orders='' then null else paid_orders end as paidorders,
        case when paid_rate='' then null else paid_rate end as paidrate,
        case when approved_orders='' then null else approved_orders end as approvedorders,
        case when approval_rate='' then null else approval_rate end as approvalrate,
        case when marketing_cost_vouchers='' then null else marketing_cost_vouchers::double precision end as marketingcostvouchers
    from stg_external_apis.gs_commercial_targets_2021_new where trunc(datum)>'2021-08-31'
    order by 1)
select
    t.datum,
    t.day_name,
    t.country,
    t.store,
    t.store_country,
    t.eom,
    t.bom,
    t.days_in_month,
    t.activesubscriptionvalue_bom,
    t.activesubscriptionvalue_eom,
    t.monthly_growth,
    t.incrementalsubsvalue,
    t.cancelledsubvalue::numeric(29,11),
    t.acquiredsubsvalue::numeric(29,11),
    t.acquiredsubs::numeric(29,11),
    t.subsperorder::numeric(29,11),
    t.paidorders,
    t.paidrate,
    t.approvedorders,
    t.approvalrate,
    t.marketingcostvouchers,
    incrementalsubsvalue as daily_growth_linear,
    sum(incrementalsubsvalue) over (PARTITION by bom,country, store order by datum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_sum,
	activesubsvalue as active_sub_value_daily_target,
    marketingcostvouchers as marketingcostvouchers_daily,
    incrementalsubsvalue as incrementalsubsvalue_daily,
    cancelledsubvalue::numeric(29,11) as cancelled_subvalue_daily,
    acquiredsubsvalue::numeric(29,11) as acquired_subvalue_daily,
    acquiredsubs::numeric(29,11) as acquired_subs_daily,
    subsperorder::numeric(29,11) as subsperorder_daily,
    paidorders::numeric(29,11) as paidorders_daily,
    paidrate as paid_rate,
    approvedorders::numeric(29,11) as approvedorders_daily,
    approvalrate as approval_rate
from targets2021 t
order by 1 desc;

-- asv targets for 2022
drop table if exists dwh.commercial_targets_daily_store_country_2022;
create table dwh.commercial_targets_daily_store_country_2022 as
with targets2022 as (
   select
        trunc(datum) as datum,
        to_char(datum::date, 'Day') as day_name,
        --one-time inputs labels fix for 2022 comm targets upload
        case  country when 'TOTAL' then 'Total' else country end as country ,
        case store  when 'Retail' then 'Partnerships' else  store end as store,
        (case store  when 'Retail' then 'Partnerships' else  store end)||'-'||(case  country when 'TOTAL' then 'Total' else country end) as store_country,
        last_day(datum)::date AS eom,
        date_trunc('month', datum) as bom,
        substring(last_day(datum),9,2)::integer as days_in_month,
        active_subs_value as activesubsvalue,
        first_value(active_subs_value) over (partition by country,store, date_trunc('month', datum) order by datum rows between unbounded preceding and unbounded following) as activesubscriptionvalue_bom,
        last_value(active_subs_value) over (partition by country,store, date_trunc('month', datum) order by datum rows between unbounded preceding and unbounded following) as activesubscriptionvalue_eom,
        coalesce(active_subs_value - activesubscriptionvalue_bom,incremental_subs_value::double precision) as monthly_growth,
        incremental_subs_value  as incremental_subs_value,
        cancelled_sub_value  as cancelledsubvalue,
        acquired_subs_value  as acquiredsubsvalue,
        acquired_subs  as acquiredsubs,
        subs_per_order  as subsperorder,
        paid_orders  as paidorders,
        paid_rate  as paidrate,
        approved_orders  as approvedorders,
        approval_rate  as approvalrate,
        marketing_cost_vouchers::double precision  as marketingcostvouchers
    from stg_external_apis.gs_commercial_targets_2022_new  where trunc(datum) BETWEEN '2022-01-01' AND '2022-05-31'
)
select
    t.datum,
    t.day_name,
    t.country,
    t.store,
    t.store_country,
    t.eom,
    t.bom,
    t.days_in_month,
    t.activesubscriptionvalue_bom,
    t.activesubscriptionvalue_eom,
    t.monthly_growth,
    t.incremental_subs_value::double precision as incrementalsubsvalue,
    t.cancelledsubvalue::numeric(29,11),
    t.acquiredsubsvalue::numeric(29,11),
    t.acquiredsubs::numeric(29,11),
    t.subsperorder::numeric(29,11),
    t.paidorders,
    t.paidrate,
    t.approvedorders,
    t.approvalrate,
    t.marketingcostvouchers,
    incremental_subs_value::double precision as daily_growth_linear,
    sum(incremental_subs_value) over (PARTITION by bom,country, store order by datum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::double precision as cumulative_sum,
    activesubsvalue as active_sub_value_daily_target,
    marketingcostvouchers as marketingcostvouchers_daily,
    incremental_subs_value::double precision as incrementalsubsvalue_daily,
    cancelledsubvalue::numeric(29,11) as cancelled_subvalue_daily,
    acquiredsubsvalue::numeric(29,11) as acquired_subvalue_daily,
    acquiredsubs::numeric(29,11) as acquired_subs_daily,
    subsperorder::numeric(29,11) as subsperorder_daily,
    paidorders::numeric(29,11) as paidorders_daily,
    paidrate as paid_rate,
    approvedorders::numeric(29,11) as approvedorders_daily,
    approvalrate as approval_rate
from targets2022 t
order by 1 desc;

-- asv targets for 2022 after review
DROP TABLE IF EXISTS dwh.commercial_targets_daily_store_country_2022_reviewed;
CREATE TABLE dwh.commercial_targets_daily_store_country_2022_reviewed AS 
with targets2022_reviewed as (
   select
        trunc(datum) as datum,
        to_char(datum::date, 'Day') as day_name,
        --one-time inputs labels fix for 2022 comm targets upload
        case  country when 'TOTAL' then 'Total' else country end as country ,
        case store  when 'Retail' then 'Partnerships' else  store end as store,
        (case store  when 'Retail' then 'Partnerships' else  store end)||'-'||(case  country when 'TOTAL' then 'Total' else country end) as store_country,
        last_day(datum)::date AS eom,
        date_trunc('month', datum) as bom,
        substring(last_day(datum),9,2)::integer as days_in_month,
        active_subs_value as activesubsvalue,
        first_value(active_subs_value) over (partition by country,store, date_trunc('month', datum) order by datum rows between unbounded preceding and unbounded following) as activesubscriptionvalue_bom,
        last_value(active_subs_value) over (partition by country,store, date_trunc('month', datum) order by datum rows between unbounded preceding and unbounded following) as activesubscriptionvalue_eom,
        coalesce(active_subs_value::NUMERIC(38,8) - activesubscriptionvalue_bom,incremental_subs_value) as monthly_growth,
        case when incremental_subs_value IS NULL then null else incremental_subs_value end as incremental_subs_value,
        case when cancelled_sub_value IS NULL then null else cancelled_sub_value end as cancelledsubvalue,
        case when acquired_subs_value IS NULL then null else acquired_subs_value end as acquiredsubsvalue,
        case when acquired_subs IS NULL then null else acquired_subs end as acquiredsubs,
        case when subs_per_order IS NULL then null else subs_per_order end as subsperorder,
        case when paid_orders IS NULL then null else paid_orders end as paidorders,
        case when paid_rate IS NULL then null else paid_rate end as paidrate,
        case when approved_orders IS NULL then null else approved_orders end as approvedorders,
        case when approval_rate IS NULL then null else approval_rate end as approvalrate,
        case when marketing_cost_vouchers IS NULL then null else marketing_cost_vouchers::double precision end as marketingcostvouchers
    from stg_external_apis.gs_commercial_targets_2022_reviewed  where trunc(datum) BETWEEN '2022-06-01' AND '2022-12-31'
)
select
    t.datum,
    t.day_name,
    t.country,
    t.store,
    t.store_country,
    t.eom,
    t.bom,
    t.days_in_month,
    t.activesubscriptionvalue_bom,
    t.activesubscriptionvalue_eom,
    t.monthly_growth,
    t.incremental_subs_value,
    t.cancelledsubvalue::numeric,
    t.acquiredsubsvalue::numeric,
    t.acquiredsubs::numeric,
    t.subsperorder,
    t.paidorders,
    t.paidrate,
    t.approvedorders,
    t.approvalrate,
    t.marketingcostvouchers,
    incremental_subs_value as daily_growth_linear,
    sum(incremental_subs_value) over (PARTITION by bom,country, store order by datum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_sum,
	activesubsvalue as active_sub_value_daily_target,
    marketingcostvouchers as marketingcostvouchers_daily,
    incremental_subs_value as incrementalsubsvalue_daily,
    cancelledsubvalue::numeric(29,11) as cancelled_subvalue_daily,
    acquiredsubsvalue::numeric(29,11) as acquired_subvalue_daily,
    acquiredsubs::numeric(29,11) as acquired_subs_daily,
    subsperorder::numeric(29,11) as subsperorder_daily,
    paidorders::numeric(29,11) as paidorders_daily,
    paidrate as paid_rate,
    approvedorders::numeric(29,11) as approvedorders_daily,
    approvalrate as approval_rate
from targets2022_reviewed t
order by 1 DESC;


-- asv targets for 2023
DROP TABLE IF EXISTS dwh.commercial_targets_daily_store_country_2023;
CREATE TABLE dwh.commercial_targets_daily_store_country_2023 AS 
with targets2023_01_04 as (
   select
        trunc(datum) as datum,
        to_char(datum::date, 'Day') as day_name,
        case  country when 'TOTAL' then 'Total' else country end as country ,
        case store  when 'Retail' then 'Partnerships' else  store end as store,
        (case store  when 'Retail' then 'Partnerships' else  store end)||'-'||(case  country when 'TOTAL' then 'Total' else country end) as store_country,
        last_day(datum)::date AS eom,
        date_trunc('month', datum) as bom,
        substring(last_day(datum),9,2)::integer as days_in_month,
        active_subs_value as activesubsvalue,
        first_value(active_subs_value) over (partition by country,store, date_trunc('month', datum) order by datum rows between unbounded preceding and unbounded following) as activesubscriptionvalue_bom,
        last_value(active_subs_value) over (partition by country,store, date_trunc('month', datum) order by datum rows between unbounded preceding and unbounded following) as activesubscriptionvalue_eom,
        coalesce(active_subs_value::NUMERIC(38,8) - activesubscriptionvalue_bom::NUMERIC(38,8),incremental_subs_value::NUMERIC(38,8)) as monthly_growth,
        case when incremental_subs_value IS NULL then null else incremental_subs_value end as incremental_subs_value,
        case when cancelled_sub_value IS NULL then null else cancelled_sub_value end as cancelledsubvalue,
        case when acquired_subs_value IS NULL then null else acquired_subs_value end as acquiredsubsvalue,
        case when acquired_subs IS NULL then null else acquired_subs end as acquiredsubs,
        case when subs_per_order IS NULL then null else subs_per_order end as subsperorder,
        case when paid_orders IS NULL then null else paid_orders end as paidorders,
        case when paid_rate IS NULL then null else paid_rate end as paidrate,
        case when approved_orders IS NULL then null else approved_orders end as approvedorders,
        case when approval_rate IS NULL then null else approval_rate end as approvalrate,
        case when marketing_cost_vouchers IS NULL then null else marketing_cost_vouchers::double precision end as marketingcostvouchers
    from stg_external_apis.gs_commercial_targets_2023_01_04  where trunc(datum)>='2023-01-01'
)
select
    t.datum,
    t.day_name,
    t.country,
    t.store,
    t.store_country,
    t.eom,
    t.bom,
    t.days_in_month,
    t.activesubscriptionvalue_bom,
    t.activesubscriptionvalue_eom,
    t.monthly_growth,
    t.incremental_subs_value::numeric,
    t.cancelledsubvalue::numeric,
    t.acquiredsubsvalue::numeric,
    t.acquiredsubs::numeric,
    t.subsperorder::numeric,
    t.paidorders,
    t.paidrate,
    t.approvedorders,
    t.approvalrate,
    t.marketingcostvouchers,
    incremental_subs_value::numeric as daily_growth_linear,
    sum(incremental_subs_value) over (PARTITION by bom,country, store order by datum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_sum,
	activesubsvalue as active_sub_value_daily_target,
    marketingcostvouchers as marketingcostvouchers_daily,
    incremental_subs_value::numeric(29,11) as incrementalsubsvalue_daily,
    cancelledsubvalue::numeric(29,11) as cancelled_subvalue_daily,
    acquiredsubsvalue::numeric(29,11) as acquired_subvalue_daily,
    acquiredsubs::numeric(29,11) as acquired_subs_daily,
    subsperorder::numeric(29,11) as subsperorder_daily,
    paidorders::numeric(29,11) as paidorders_daily,
    paidrate as paid_rate,
    approvedorders::numeric(29,11) as approvedorders_daily,
    approvalrate as approval_rate
from targets2023_01_04 t
order by 1 DESC
;



-- Union results



drop table if exists dwh.commercial_targets_daily_store_country;
create table dwh.commercial_targets_daily_store_country as
select * from dwh.commercial_targets_daily_store_country_2020
union all
select * from dwh.commercial_targets_daily_store_country_2021
union all
select * from dwh.commercial_targets_daily_store_country_2022
union all
select * from dwh.commercial_targets_daily_store_country_2022_reviewed
union all
select * from dwh.commercial_targets_daily_store_country_2023;