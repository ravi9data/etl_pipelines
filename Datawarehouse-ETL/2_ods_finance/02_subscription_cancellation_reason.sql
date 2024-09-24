drop table if exists ods_production.subscription_cancellation_reason;
create table ods_production.subscription_cancellation_reason as 
with Lost_assets_subs as (
select
 subscription_id,
 asset_allocation_id,
 lost_reason,
 updated_date
from ods_production.asset a
where asset_status_original = 'LOST'
 and asset_allocation_id is not null
order by 3 DESC)
,a as(
 select
  s.subscription_id,
  s.cancellation_date,
  s.cancellation_note,
  s.cancellation_reason,
  s.order_id,
  sa.allocated_assets,
  s.cancellation_reason_dropdown,
  paid_subscriptions,
  refunded_subscriptions,
  	case
    when s.allocation_status = 'PENDING ALLOCATION'
     AND (lower(cancellation_reason) like '%product%' or lower(cancellation_note) like '%product%' or lower(cancellation_reason) like '%supplier%')
     AND coalesce(sa.allocated_assets,0)=0
      then 'CANCELLED BEFORE ALLOCATION - PROCUREMENT'
	    when s.allocation_status = 'PENDING ALLOCATION'
     AND coalesce(sa.allocated_assets,0)=0
     And (lower(cancellation_reason) like '%customer%' or lower(cancellation_reason_dropdown) like '%customer%')
      then 'CANCELLED BEFORE ALLOCATION - CUSTOMER REQUEST'
    when s.allocation_status = 'PENDING ALLOCATION'
     AND (coalesce(sa.allocated_assets,0)=0
           or sa.allocated_assets is null)
      then 'CANCELLED BEFORE ALLOCATION - OTHERS'
    WHEN s.allocation_status = 'PENDING ALLOCATION'
     AND sa.allocated_assets = 1
     and (a.ready_to_ship_at is null or a.shipment_at is null)
      THEN 'CANCELLED BEFORE SHIPMENT'
    WHEN (s.allocation_status = 'ALLOCATED'
     AND sa.allocated_assets = 1
     and sa.delivered_assets > 0
     and la.lost_reason is not null )
      or s.cancellation_reason in ('delivery lost')
      THEN 'LOST DURING RETURNS'
	 WHEN (s.allocation_status = 'ALLOCATED'
     AND sa.allocated_assets = 1
     and sa.delivered_assets = 0
     and la.lost_reason is not null )
      or s.cancellation_reason in ('delivery lost')
      THEN 'LOST DURING OUTBOUND'      
    WHEN s.allocation_status = 'ALLOCATED'
     AND sa.allocated_assets = 1
     and (a.failed_delivery_at is not null
	     Or 
		 (delivered_assets = 0 and   returned_assets > 0)) 
      then 'FAILED DELIVERY'
    WHEN s.allocation_status = 'ALLOCATED'
     AND (S.debt_collection_handover_date IS NOT NULL
         OR (sa.debt_collection_assets > 0)
         OR s.cancellation_reason ILIKE '%Debt collection%'
         OR s.cancellation_reason_dropdown ILIKE '%Debt collection%')
     THEN 'DEBT COLLECTION'
    WHEN s.allocation_status = 'ALLOCATED'
     AND sa.allocated_assets = 1
     and a.is_last_allocation_per_asset IS TRUE
     AND ast.asset_status_original = 'SOLD'
     AND sc.asset_purchase_amount_paid <= 1
     and sc.customer_bought_payments > 0 
     --and sc.asset_purchase_amount_paid <> 0
      then 'SOLD 1-EUR'
    WHEN s.allocation_status = 'ALLOCATED'
     AND  sa.allocated_assets = 1
     and a.is_last_allocation_per_asset IS TRUE
     AND ast.asset_status_original = 'SOLD'
     AND sc.asset_purchase_amount_paid > 1
      THEN 'SOLD EARLY'
    WHEN s.allocation_status = 'ALLOCATED'
     AND S.replacement_attempts > 0
      THEN 'REPLACEMENT'
     WHEN (s.allocation_status = 'ALLOCATED'
     AND sa.allocated_assets = 1
     AND a.delivered_at IS NOT NULL
     AND  sc.paid_subscriptions = 0)
     or (s.allocation_status = 'ALLOCATED'
     AND sa.allocated_assets = 1
     AND ast.asset_status_original IS NULL)
     or lower(s.cancellation_reason) in ('revocation','widerruf')
     or lower(s.cancellation_reason_dropdown) in ('revocation','widerruf')
     or (sa.allocated_assets = 1 and a.widerruf_claim_date is not null and net_subscription_revenue_paid = 0)
      then 'REVOCATION'
     when s.cancellation_reason like ('%7 day%') or s.cancellation_reason like ('%testmiete%')
      then '7 DAY TRIAL'
    WHEN s.allocation_status = 'ALLOCATED'
     AND sa.allocated_assets = 1
     AND a.return_delivery_date IS NOT NULL
     AND ((sc.paid_subscriptions+sc.refunded_subscriptions+sc.chargeback_subscriptions) < s.minimum_term_months)
     AND (lower(s.cancellation_note) like '%damage%' or lower(cancellation_reason) like '%damage%' or 
	     					cancellation_reason like ('%defekt%') or a.issue_reason in ('Asset Damage','Incomplete','Limited Function','DOA','Used Condition','Not Refurbished','Locked Device'))
      THEN 'RETURNED EARLY - Asset Damage'
	    WHEN s.allocation_status = 'ALLOCATED'
     AND sa.allocated_assets = 1
     AND a.return_delivery_date IS NOT NULL
     AND (sc.paid_subscriptions < s.minimum_term_months)
     and failed_subscriptions > 0 
     then 'RETURNED EARLY - Failed Payments'
    WHEN s.allocation_status = 'ALLOCATED'
     AND sa.allocated_assets = 1
     AND a.return_delivery_date IS NOT NULL
     AND ((sc.paid_subscriptions+sc.refunded_subscriptions+sc.chargeback_subscriptions) < s.minimum_term_months)
      THEN 'RETURNED EARLY'
    WHEN s.allocation_status = 'ALLOCATED'
     AND sa.allocated_assets = 1
     AND a.return_delivery_date IS NOT NULL
     AND ((sc.paid_subscriptions+sc.refunded_subscriptions+sc.chargeback_subscriptions) = s.minimum_term_months)
      THEN 'RETURNED ON TIME'
    WHEN s.allocation_status = 'ALLOCATED'
     AND sa.allocated_assets = 1
     AND a.return_delivery_date IS NOT NULL
     AND ((sc.paid_subscriptions+sc.refunded_subscriptions+sc.chargeback_subscriptions) > s.minimum_term_months)
      THEN 'RETURNED LATER'
    WHEN s.allocation_status = 'ALLOCATED'
     AND sa.allocated_assets = 1
     AND ((a.ready_to_ship_at IS NULL) OR (a.shipment_at IS NULL))
      THEN 'CANCELLED BEFORE SHIPMENT'
    WHEN s.allocation_status = 'ALLOCATED'
     AND sa.allocated_assets > 1
     AND Sa.delivered_assets > 0
     AND Sa.outstanding_assets = 0
     AND COALESCE(S.CROSS_SALE_ATTEMPTS,0) = 0
      THEN 'RETURNED MULTIPLE ALLOCATIONS'
    WHEN (s.allocation_status = 'ALLOCATED'
     AND sa.allocated_assets > 1
     AND Sa.allocated_assets = sa.failed_delivery_assets)
      or s.cancellation_reason in ('failed delivery')
      THEN 'FAILED DELIVERY'
    ELSE 'UNCLASSIFIED/OTHERS'
    END AS cancellation_reason_new,
   max(greatest(s.updated_date,sc.updated_at,sa.updated_at,a.updated_at,ast.updated_date,la.updated_date)) as updated_at,
  listagg(a.issue_reason,'-') as issue_reason
   from ods_production.subscription s
    LEFT JOIN ods_production.subscription_cashflow sc
     ON s.subscription_id = sc.subscription_id
    LEFT JOIN ods_production.subscription_assets sa
     ON s.subscription_id = sa.subscription_id
    LEFT JOIN ods_production.allocation a
     on s.subscription_id = a.subscription_id
    LEFT JOIN ods_production.asset ast
     on ast.asset_id = a.asset_id
    LEFT join lost_assets_subs la
     on la.asset_allocation_id = a.allocation_id
 WHERE s.status = 'CANCELLED'
group by 1,2,3,4,5,6,7,8,9,10)
select
 subscription_id,
 order_id,
 allocated_assets,
 cancellation_date,
 cancellation_reason,
 cancellation_reason_dropdown,
 cancellation_note,
 cancellation_reason_new,
 issue_reason,
 case
  when cancellation_reason_new in (
   'SOLD 1-EUR'
  ,'SOLD EARLY'
  ,'REPLACEMENT'
  ,'RETURNED ON TIME'
  ,'RETURNED EARLY - Asset Damage'
  ,'RETURNED EARLY - Failed Payments'
  ,'RETURNED EARLY'
  ,'RETURNED LATER'
  ,'RETURNED MULTIPLE ALLOCATIONS'
  ,'LOST DURING RETURNS'
  ,'UNCLASSIFIED/OTHERS') AND COALESCE(cancellation_reason,'') <> 'handed over to dca'
   then 'customer request'
  when cancellation_reason_new in (
   'CANCELLED BEFORE ALLOCATION - PROCUREMENT'
   ,'CANCELLED BEFORE ALLOCATION - CUSTOMER REQUEST'
   ,'CANCELLED BEFORE ALLOCATION - OTHERS'
  ,'CANCELLED BEFORE SHIPMENT'
  ,'LOST DURING OUTBOUND'
  ,'FAILED DELIVERY'
  ,'7 DAY TRIAL'
  ,'REVOCATION'
  ,'CANCELLED BEFORE SHIPMENT')
   then 'failed delivery'
  when cancellation_reason_new in (
   'DEBT COLLECTION') OR cancellation_reason = 'handed over to dca'
  end as cancellation_reason_churn,
  (case
    when cancellation_reason_new in (
   'REVOCATION')
    then 'true'
   else 'false'
  end)::varchar(24) as is_widerruf,
  paid_subscriptions,
  refunded_subscriptions,
  updated_at
from a;

GRANT SELECT ON ods_production.subscription_cancellation_reason TO tableau;
