drop table if exists table tgt_dev.spv_feb20_test2;
create table tgt_dev.spv_feb20_test2 as 
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
,reporting_month as (
select distinct 
 spv.asset_id,
 spv.reporting_date,
 sum(coalesce(subscription_revenue_paid,0)) as subscription_revenue_paid_reporting_month,
 sum(coalesce(chargeback_paid,0)
  +coalesce(refunds_paid,0)) as refunds_chb_paid_reporting_month,
 sum(coalesce(repair_cost_paid,0)+
  coalesce(shipment_cost_paid,0)+
  coalesce(additional_charge_paid,0)) as additional_charges_paid_reporting_month,
 sum(coalesce(debt_collection_paid,0)) as debt_collection_paid_reporting_month
from dwh.asset_collection_curves  acc
inner join ods_production.spv_report_master spv
 on spv.asset_id=acc.asset_Id
 and date_trunc('month',spv.reporting_date)::date=date_trunc('month',acc.datum_month)::date
 group by 1,2
)
 ,lifetime as (
select distinct 
spv.asset_id,
spv.reporting_date,
sum(coalesce(subscription_revenue_paid,0)) as subscription_revenue_paid_lifetime,
sum(coalesce(chargeback_paid,0)
 +coalesce(refunds_paid,0)) as refunds_chb_paid_lifetime,
sum(coalesce(repair_cost_paid,0)
 +coalesce(shipment_cost_paid,0)
 +coalesce(additional_charge_paid,0)) as additional_charges_paid_lifetime,
sum(coalesce(debt_collection_paid,0)) as debt_collection_paid_lifetime
from dwh.asset_collection_curves  acc
left join ods_production.spv_report_master spv
 on spv.asset_id=acc.asset_Id
 and date_trunc('month',spv.reporting_date)>=date_trunc('month',acc.datum_month)
 group by 1,2)
 -------------------------------------------------------------------------------
,active_value as (
select distinct 
 last_day(date_trunc('month',s."date"))::date AS reporting_month,
 s.subscription_id,
 a.asset_id,
 s.subscription_value,
 max(case when s.start_date::date=s.date::date then 1 else 0 end) as is_acquired
from master.subscription_historical s 
left join master.allocation_historical a 
 on s.subscription_id=a.subscription_id 
 and s."date"=a."date"
where date_trunc('month',s.date)='2020-02-01'
and a.allocation_status_original not in ('CANCELLED')
and s.status='ACTIVE'
group by 1,2,3,4)
-------------------------------------------------------------------------------
,active_value_final as (
select 
asset_id,
reporting_month,
sum(subscription_value) as active_subscription_value,
sum(case when is_acquired=1 then subscription_value end) as acquired_subscription_value,
sum(case when is_acquired=0 then subscription_value end) as rollover_subscription_value
from active_value 
group by 1,2
    )
-------------------------------------------------------------------------------
,cancelled_value as (
select distinct 
 last_day(date_trunc('month',s."date"))::date AS reporting_month,
 s.subscription_id,
 a.asset_id,
 s.subscription_value
from master.subscription_historical s 
left join master.allocation_historical a 
 on s.subscription_id=a.subscription_id 
 and s."date"=a."date"
where date_trunc('month',s.date)='2020-02-01'
and a.allocation_status_original not in ('CANCELLED')
and s.date::Date=s.cancellation_date::Date
)
-------------------------------------------------------------------------------
,cancelled_value_final as (
select 
 asset_id,
 reporting_month,
 sum(subscription_value) as cancelled_subscription_value
from cancelled_value 
group by 1,2
    )
,sale as (
select * from 
(
select 
max(dateoftransfer::date) over (partition by assetid) as max_transfer_date, 
o.*
from stg_external_apis.asset_sale_opco_finco o 
) a where max_transfer_date=dateoftransfer::date)
-------------------------------------------------------------------------------
SELECT distinct 
    spv.reporting_date,
    spv.asset_id,
    h.subscription_id,
    spv.asset_name,
--    h.dpd_bucket,
case  
when    last_allocation_dpd=0 then 'PERFORMING'
when    last_allocation_dpd>0 and last_allocation_dpd<31 then 'OD 1-30'
when    last_allocation_dpd>30 and last_allocation_dpd<61 then 'OD 31-60'
when    last_allocation_dpd>60 and last_allocation_dpd<91 then 'OD 61-90'
when    last_allocation_dpd>90 and last_allocation_dpd<121 then 'OD 91-120'
when    last_allocation_dpd>120 and last_allocation_dpd<151 then 'OD 121-150'
when    last_allocation_dpd>150 and last_allocation_dpd<180 then 'OD 151-180'
when    last_allocation_dpd>180 then 'OD 180+'
when h.asset_status_original='SOLD' then 'SOLD'
when h.asset_status_original='LOST' or h.asset_status_original='LOST SOLVED' then 'LOST'
when r.active_subscriptions_eom>0 then 'PERFORMING'
else    'IN STOCK' 
END as dpd_bucket,
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
    case 
     when date_trunc('month',spv.sold_date::Date)=date_trunc('month',spv.reporting_date) 
      then 1 
       else 0 
    end as is_sold_reporting_month,
    spv.sold_price,
    spv.asset_status_original,
    h.subscription_revenue_due as subscription_revenue_due_lifetime,
    h.subscription_revenue as subscription_revenue_paid_lifetime,
    cfl.subscription_revenue_paid_lifetime as subscription_revenue_paid_lifetime_qa,
    s.subscription_revenue_paid as subscription_revenue_paid_current_subscription,
    h.subscription_revenue_current_month as subsription_revenue_paid_reporting_month,
    coalesce(repair_cost_paid,0)+coalesce(shipment_cost_paid,0)+coalesce(additional_charge_paid,0) as additional_charge_paid_lifetime,
    cfr.additional_charges_paid_reporting_month,
    cfl.refunds_chb_paid_lifetime,
    cfr.refunds_chb_paid_reporting_month,
    cfr.debt_collection_paid_reporting_month,
    cfl.debt_collection_paid_lifetime,
    r.active_subscriptions_bom,
    r.active_subscriptions_eom,
    r.active_subscriptions,
    avf.active_subscription_value,
    r.acquired_subscriptions,
    avf.acquired_subscription_value,
    r.rollover_subscriptions,
    avf.rollover_subscription_value,
    r.cancelled_subscriptions,
    cvf.cancelled_subscription_value,
    h.capital_source_name,
    sale.saleno as sale_batch,
    sale.dateoftransfer as sale_date,
    case 
     when date_trunc('month',sale.dateoftransfer::Date)=date_trunc('month',spv.reporting_date) 
      then 1 
       else 0 
    end as is_purchased_reporting_month
from ods_production.spv_report_master spv 
left join master.asset_historical AS h 
 on spv.asset_id=h.asset_id 
 and spv.reporting_date::Date=h.date::date
left join master.subscription_historical AS s  
 on h.subscription_id=s.subscription_id 
 and h.date::Date=s.date::date
left join ods_production.supplier ss 
 on ss.supplier_name=h.supplier
left join sale 
 on sale.assetid=h.asset_id
left join sub_recon r 
 on r.asset_id=h.asset_id 
 and h.date::date=r.reporing_month::date
left join reporting_month cfr 
 on cfr.asset_id=spv.asset_id 
 and cfr.reporting_date::Date=spv.reporting_date::Date
left join lifetime cfl 
 on cfl.asset_id=spv.asset_id 
 and cfl.reporting_date::Date=spv.reporting_date::Date
left join active_value_final avf 
 on avf.asset_id=spv.asset_id
 and avf.reporting_month::Date=spv.reporting_date::Date
left join cancelled_value_final cvf 
 on cvf.asset_id=spv.asset_id
 and cvf.reporting_month::Date=spv.reporting_date::Date
where spv.reporting_date ='2020-02-29 00:00:00.000000'
;
