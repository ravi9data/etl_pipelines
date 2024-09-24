truncate dm_recommerce.recommerce_stages;

insert into dm_recommerce.recommerce_stages
with refurbishment_data_latest_record as(
select sub_q.sent_to_recommerce,
sub_q.seriennummer,
sub_q.stellplatz,
sub_q.spl_position,
'Wemalo' as _3pl
from(
select widm.reporting_date as last_report_date,
widm.seriennummer,
widm.stellplatz,
widm.spl_position,
min(reporting_date) over( partition by seriennummer) as sent_to_recommerce,
--rn is created to use last event for the assets sent to recommerce
row_number() over(partition by seriennummer order by reporting_date desc) as rn
from dm_recommerce.wemalo_inventory_daily_movement widm 
where UPPER(stellplatz) like '%SELL-NR%' or
UPPER(stellplatz) like 'B2BSELL%' or
left(UPPER(stellplatz),6)='INV-05' or
left(UPPER(stellplatz),6)='INV-06' or
left(UPPER(stellplatz),6)='INV-07'or
spl_position in('Irrepearable (Technical)','Irrepearable (Optical)','B2B Bulky') ) as sub_q
where rn=1),

wemalo_recommerce_data as(
select r.*,
ri.partner,
ri.sold_date,
ri.sales_price_net,
ri.sales_price_gross,
ri.invoice_type
from refurbishment_data_latest_record r
left join dm_recommerce.recommerce_b2b_b2c_invoices ri on ri.serial_number = r.seriennummer),

ingram_base_data as(
select date_of_processing,
serial_number,
disposition_code,
order_number,
status_code from(
select ii.source_timestamp as date_of_processing,
ii.serial_number,
ii.disposition_code,
ii.status_code,
ii.order_number,
steps_4r,
--rn is used to get first event of each asset based on disposition_code and status_code
row_number() over(partition by serial_number, ii.disposition_code, ii.status_code order by source_timestamp) as rn,
--rn_for_processed is used to get last event of processed asset based on disposition_code and status_code
row_number() over(partition by serial_number, ii.disposition_code, ii.status_code order by source_timestamp desc) as rn_for_processed
from recommerce.ingram_micro_send_order_grading_status ii
left join recommerce.ingram_kpi_mapping ikm on ii.status_code = ikm.status_code and ii.disposition_code =ikm.disposition_code
) where (case when steps_4r='Processed recommerce' then rn_for_processed else rn end)=1 and serial_number is not null
),

ingram_recommerce_data as(
select distinct ii.date_of_processing,
ii.serial_number,
ii.disposition_code || '--> ' || ii.status_code as status,
steps_4r,
'Ingram'    as _3pl
from ingram_base_data ii
left join recommerce.ingram_kpi_mapping ikm on ii.status_code = ikm.status_code and ii.disposition_code =ikm.disposition_code
where steps_4r like '%recommerce%'
),


wemalo_in_process_base as(
select sent_to_recommerce as fact_date,
seriennummer as serial_number,
stellplatz,
spl_position as status,
_3pl
from wemalo_recommerce_data
where sold_date is null
),

fact_dates as(
select dd.datum 
from public.dim_dates dd where dd.datum <=current_date and dd.datum>'2021-11-01'),--based on stakeholder's request, we have a date filter here

wemalo_in_process as(
select *
from fact_dates fd
left join wemalo_in_process_base w on fd.datum>=w.fact_date::date
),

recommerce_incoming as(
select sent_to_recommerce as fact_date,
seriennummer as serial_number,
stellplatz,
spl_position as status,
_3pl
from refurbishment_data_latest_record
union all
select date_of_processing as fact_date,
serial_number,
null::text as stellplatz,
status,
_3pl
from ingram_recommerce_data
where steps_4r='Incoming recommerce'
),

recommerce_in_process as(
select datum as fact_date,
serial_number,
stellplatz,
status,
_3pl
from wemalo_in_process
union all
select source_timestamp as fact_date,
serial_number,
null::text as stellplatz,
disposition_code || '--> ' || status_code as status,
'Ingram' as_3pl from(
select ii.*,
ikm.steps_4r,
--the asset shouldn't be counted as In Process if it's processed in the same month
last_value(steps_4r) over(partition by serial_number, date_trunc('month',source_timestamp) order by source_timestamp asc
rows between unbounded preceding and unbounded following) as last_stage
from recommerce.ingram_micro_send_order_grading_status ii
left join recommerce.ingram_kpi_mapping ikm on ii.status_code = ikm.status_code and ii.disposition_code =ikm.disposition_code
) where steps_4r='In process recommerce' and last_stage<>'Processed recommerce'
),

recommerce_processed as(
--Assets processed in Wemalo and have invoices
select sold_date as fact_date,
serial_number,
null::text as stellplatz,
null::text as status,
'Wemalo' as _3pl
from dm_recommerce.recommerce_b2b_b2c_invoices 
where sold_date is not null and serial_number not in(select distinct serial_number from ingram_recommerce_data)
union all

--Assets processed in Ingram and have invoices
select sold_date as fact_date,
serial_number,
null::text as stellplatz,
null::text as status,
'Ingram' as _3pl
from dm_recommerce.recommerce_b2b_b2c_invoices 
where sold_date is not null and serial_number in(select distinct serial_number from ingram_recommerce_data)
union all

--Assets processed in Ingram but don't have invoices yet
select date_of_processing as fact_date,
serial_number,
null::text as stellplatz,
status,
_3pl
from ingram_recommerce_data
where steps_4r='Processed recommerce'
and serial_number in(
select distinct serial_number from ingram_recommerce_data
minus
select distinct serial_number from dm_recommerce.recommerce_b2b_b2c_invoices
)
),

all_data as(
select *,
min(case when kpi='Incoming' then fact_date end) over(partition by serial_number) as in_process_start_date,
max(case when kpi='In Process' then fact_date end) over(partition by serial_number, date_trunc('month', fact_date)) as last_day_in_process_per_month,
coalesce((max(case when kpi='Processed' then fact_date end) over(partition by serial_number)), current_date) as in_process_end_date,
datediff('day',in_process_start_date,last_day_in_process_per_month) as days_in_process,
datediff('day',in_process_start_date,in_process_end_date) as days_in_process_for_processed_assets
from(
select *, 'Incoming'   as KPI from recommerce_incoming
union all
select *, 'In Process' as KPI from recommerce_in_process
union all
select *, 'Processed'  as KPI from recommerce_processed))

select d.*, a.asset_id
from all_data d 
left join master.asset a on d.serial_number =a.serial_number;