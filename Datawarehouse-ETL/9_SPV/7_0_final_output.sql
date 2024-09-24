create table dm_finance.spv_report_202002 as 
with sub_recon as (
select 
last_day(date_trunc('month',s."date"))::date as reporing_month, 
a.asset_id, 
count(distinct 
	case 
		when status='ACTIVE' 
		 and s."date"=date_trunc('month',s."date")
	   then s.subscription_id 
	end) as active_subscriptions_bom,
count(distinct 
	case 
		when status='ACTIVE' 
		 and s."date"=last_day(date_trunc('month',s."date"))
	   then s.subscription_id 
	end) as active_subscriptions_eom,
count(distinct 
	case 
		when status='ACTIVE' 
	   then s.subscription_id 
	end) as active_subscriptions,
count(distinct 
	case 
		when status='ACTIVE' 
		 and start_date::Date=s.date::date 
		then s.subscription_id 
	end) as acquired_subscriptions,
count(distinct 
	case 
		when status='ACTIVE' 
		 and start_date::Date<date_trunc('month',s."date") 
		then s.subscription_id 
	end) as rollover_subscriptions,	
count(distinct 
 	case 
 		when cancellation_date::date=s."date" 
 		 then s.subscription_id 
 	end) as cancelled_subscriptions
from master.subscription_historical s 
left join master.allocation_historical a 
 on s.subscription_id=a.subscription_id 
 and s."date"=a."date"
where date_trunc('month',s.date)='2020-02-01'
and a.allocation_status_original not in ('CANCELLED')
group by 1,2)
,sale as (
select * from 
(
select 
max(dateoftransfer::date) over (partition by assetid) as max_transfer_date, 
o.*
from stg_external_apis.asset_sale_opco_finco o 
) a where max_transfer_date=dateoftransfer::date)
SELECT distinct 
    spv.reporting_date,
    spv.asset_id,
    h.subscription_id,
    spv.asset_name,
    h.dpd_bucket,
    category,
    subcategory,
    spv.serial_number,
    spv.product_sku,
    spv."condition",
    case when ss.agan_asset_supplier='true' then 'AGAN' else 'NEW' end as condition_on_Purchase,
    country_code,
    spv.city,
    spv.postal_code,
    spv.purchased_date,
    spv.initial_price,
    spv.delivered_allocations,
    spv.returned_allocations,
    days_in_stock,
    m_since_last_valuation_price,
    final_price,
    valuation_method,
    spv.sold_date,
    spv.sold_price,
    spv.asset_status_original,
    h.subscription_revenue_due as subscription_revenue_due_lifetime,
    h.subscription_revenue as subscription_revenue_paid_lifetime,
    s.subscription_revenue_paid as subscription_revenue_paid_current_subscription,
    h.subscription_revenue_current_month as subsription_revenue_paid_reporting_month,
    coalesce(repair_cost_paid,0)+coalesce(shipment_cost_paid,0)+coalesce(additional_charge_paid,0) as additional_charge_paid_lifetime,
    r.active_subscriptions_bom,
    r.active_subscriptions_eom,
    r.active_subscriptions,
    r.acquired_subscriptions,
    r.rollover_subscriptions,
    r.cancelled_subscriptions,
    h.capital_source_name,
    sale.saleno as sale_batch,
    sale.dateoftransfer as sale_date
from ods_production.spv_report_master spv 
left join master.asset_historical AS h 
 on spv.asset_id=h.asset_id 
 and spv.reporting_date::Date=h.date::date
left join master.subscription_historical AS s  
 on h.subscription_id=s.subscription_id 
 and h.date::Date=s.date::date
left join sub_recon r 
 on r.asset_id=h.asset_id 
 and h.date::date=r.reporing_month::date
left join ods_production.supplier ss 
 on ss.supplier_name=h.supplier
left join sale 
 on sale.assetid=h.asset_id
where reporting_date ='2020-02-29 00:00:00.000000'
;