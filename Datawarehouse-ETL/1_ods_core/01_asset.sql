drop table if exists ods_production.asset;
create table ods_production.asset AS
WITH dhl_lost AS (
  SELECT
    a_1.id AS asset_id,
    max(GREATEST(a_1.lastmodifieddate, a_1.systemmodstamp, ac.lastmodifieddate, ac.systemmodstamp)) as updated_at,
    CASE
      WHEN a_1.status :: text = 'SOLD' :: text
      AND ac.name :: text = 'DHL' :: text THEN 'LOST SOLVED' :: text
      ELSE NULL :: text
    END AS status
  FROM stg_salesforce.asset a_1
  LEFT JOIN stg_salesforce.account ac ON a_1.accountid :: text = ac.id :: text
  WHERE
    a_1.status :: text = 'SOLD' :: text
    AND ac.name :: text = 'DHL' :: text
    group by a_1.id, ac.name, a_1.status
)
,delivered_allocations as (
  select
    asset_id,
    max(updated_at) as updated_at,
    count(
      distinct case
        when delivered_at is not null then allocation_id
      end
    ) as delivered_allocations
  from ods_production.allocation
  group by
    1
)
,billing_payments as (
   select 
      am.asset_id,
      a.uuid as payment_id,
      a."group" as payment_group_id,
      a.contractid as subscription_id,
      a.status AS payment_status,
      a.taxincluded,
      a.status,
   CASE WHEN taxincluded=FALSE THEN 
   sum(COALESCE(NULLIF(JSON_EXTRACT_PATH_TEXT(
		JSON_EXTRACT_PATH_TEXT(
				JSON_SERIALIZE(payment_group_tax_breakdown[0])
      ,'total')
   ,'in_cents'),'')::decimal(30,2),0)/100) ELSE sum(amount) END  AS paid_amount
   from oltp_billing.payment_order a 
   LEFT JOIN ods_production.asset_subscription_mapping  am 
   ON am.subscription_id_migrated=a.contractid
   AND rx_last_allocation_per_sub = 1
   where a.paymenttype='purchase'
     AND IS_VALID_JSON(JSON_SERIALIZE(payment_group_tax_breakdown[0]))
     AND status IN ('paid','partial paid')
   GROUP BY 1,2,3,4,5,6,7
   )
,billing_transactions as (
   select 
      account_to as payment_group_id,
      created_at as transaction_date,
      status as transaction_status,
      amount AS transaction_paid_amount,
      row_number() over (partition by account_to order by created_at desc) as rn
   from oltp_billing.transaction
   WHERE status = 'success'
   )
,new_infra_sold_date_pre AS (
select 
   a.*,
   b.transaction_status,
   b.transaction_paid_amount ,
   b.transaction_date,
   case when b.transaction_status='success' then b.transaction_date else null end as sold_date,
   ROW_NUMBER ()OVER(PARTITION BY a.subscription_id ORDER BY sold_date DESC ) idx--TO eliminate mulitple purchase date FOR one asset)
from billing_payments a
left join billing_transactions b 
on a.payment_group_id=b.payment_group_id
and b.rn=1
)
,new_infra_sold_Date AS(
   SELECT 
      Asset_id,
      subscription_id,
      sold_date::date,
      CASE WHEN payment_status='partial paid' THEN transaction_paid_amount ELSE paid_amount END AS paid_amount
   FROM new_infra_sold_date_pre 
   WHERE TRUE  --subscription_id ='F-BB946YP9'
   AND   idx=1
   )
,asset_history_sold_date_pre AS (----Sold date FROM Asset History
   SELECT 
      distinct assetid as asset_id,
      LAST_VALUE (createddate) OVER (PARTITION BY assetid order BY createddate ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sold_date,
      LAST_VALUE (oldvalue) OVER (PARTITION BY assetid order BY createddate ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_old_status,
      LAST_VALUE (newvalue) OVER (PARTITION BY assetid order BY createddate ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_new_status
   FROM stg_salesforce.asset_history ah
   WHERE field='Status'
   )
,asset_history_sold_date AS (
   SELECT 
      ah.*,
      a.status
   FROM asset_history_sold_date_pre ah
   LEFT JOIN stg_salesforce.asset a 
      ON ah.asset_id=a.id
   WHERE TRUE 
      AND ah.last_new_status ='SOLD'
      AND a.status ='SOLD'
)
,payment_sold_date AS (
   SELECT 
      ap.asset__c AS asset_id,
      max(ap.date_paid__c) AS sold_date,
      sum(CASE WHEN Coalesce(CASE WHEN ap.status__c='CHARGED BACK' THEN ap.amount_f_due__c ELSE ap.amount_paid__c end,0)-COALESCE(ap.amount_refunded__c,0)=0 
         THEN 
            NULL ELSE Coalesce(CASE WHEN ap.status__c='CHARGED BACK' THEN ap.amount_f_due__c ELSE ap.amount_paid__c end,0)-
               Coalesce(ap.amount_refunded__c,0)END)  AS sold_price
   from stg_salesforce.asset_payment__c ap
   LEFT JOIN stg_salesforce.asset a 
      ON a.id=ap.asset__c 
   WHERE TRUE
      AND ap.type__c in ('GROVER SOLD','CUSTOMER BOUGHT')
      AND ap.status__c IN('PAID','CHARGED BACK')
      AND a.status ='SOLD' 
      GROUP BY 1
)
,dc_cases AS (
   SELECT 
      ap.asset__c AS asset_id ,
      max(
       CASE
          WHEN ap.type__c in ('DEBT COLLECTION') AND ap.date_paid__c IS NOT NULL
               AND ap.status__c = 'PAID'
          THEN ap.date_paid__c END) AS max_dc_sold_date,
      sum(CASE
          WHEN ap.type__c in ('DEBT COLLECTION') AND ap.date_paid__c IS NOT NULL
              AND ap.status__c = 'PAID'
         THEN ap.amount_paid__c END) AS sold_amount,
      sum(CASE
          WHEN ap.type__c in ('DEBT COLLECTION') AND ap.date_paid__c IS NOT NULL
             AND ap.status__c = 'PAID'
         THEN COALESCE(ap.amount_refunded__c,0) END) AS sold_refunded,
         sold_amount-sold_refunded AS sold_price
   FROM stg_salesforce.asset_payment__c ap
   INNER JOIN asset_history_sold_date ah
      ON ah.asset_id=ap.asset__c 
   WHERE TRUE 
      AND ah.status ='SOLD'
      AND ah.last_new_status ='SOLD'
   GROUP BY 1     
)
,lost_date AS (
   SELECT DISTINCT
     ah.assetid 
     ,MAX(ah.createddate) AS lost_date
   FROM stg_salesforce.asset_history ah
     INNER JOIN stg_salesforce.asset a
       ON ah.assetid = a.id
   WHERE TRUE 
     AND a.status = 'LOST'
     AND ah.field = 'Status'
     AND ah.newvalue = 'LOST'
   GROUP BY 1
)
,write_off_date AS (
   SELECT DISTINCT
     ah.assetid 
     ,MAX(ah.createddate) AS write_off_date
   FROM stg_salesforce.asset_history ah
     INNER JOIN stg_salesforce.asset a
       ON ah.assetid = a.id
   WHERE TRUE 
     AND a.status LIKE '%WRITTEN OF%'
     AND ah.field = 'Status'
     AND ah.newvalue LIKE '%WRITTEN OF%'
   GROUP BY 1
)
,in_stock_date_pre as (
  select 
    assetid, 
    case when newvalue in ('TRANSFERRED TO WH', 'INBOUND UNALLOCABLE') then 'IN STOCK' else newvalue  end as calcvalue,
    date_trunc('day', case when calcvalue = 'IN STOCK'
                            and calcvalue <> lead(calcvalue) over (partition by assetid order by createddate desc) 
                           then createddate end ) as in_stock_at
  from stg_salesforce.asset_history 
  where field = 'Status'
)
, in_stock_date as (
  select 
    assetid as asset_id,
    max(in_stock_at) as _in_stock_at
  from in_stock_date_pre
  group by 1
)
,last_subscription AS (
   SELECT 
      asset_id,
      subscription_id,
      allocation_id,
      customer_id,
      country_name,
      allocation_status_original
   FROM ods_production.asset_subscription_mapping
   WHERE rx_last_allocation_per_asset=1
   AND allocation_status_original NOT IN ('RETURNED','CANCELLED') 
   )
select
  a.id as asset_id,
  COALESCE(ls.allocation_id,a.asset_allocation__c) as asset_allocation_id,
  COALESCE(ls.subscription_id,aa.subscription__c) as subscription_id,
  a.f_product_sku_variant__c as variant_sku,
  a.f_product_sku_product__c as product_sku,
  (v.ean) as product_ean,
  a.serialnumber as serial_number,
  COALESCE(CUST.spree_customer_id__c,ls.customer_id) as customer_id,
  a.name as asset_name,
  p.product_name,
  p.category_name,
  p.subcategory_name,
  p.brand,
  a.currency__c as currency,
  a.amount_rrp__c as amount_rrp,
  a.cost_price__c as initial_price,
   COALESCE(a.purchased__c :: date, a.createddate :: date) as purchased_date,
  (
    COALESCE('now' :: text :: date :: timestamp) :: date - COALESCE(
      a.purchased__c,
      a.invoice_date__c :: timestamp,
      a.createddate
    ) :: date
  ) / 30 + 1 AS months_since_purchase,
  COALESCE('now' :: text :: date :: timestamp) :: date - COALESCE(
    a.purchased__c,
    a.invoice_date__c :: timestamp,
    a.createddate
  ) :: date AS days_since_purchase,
   COALESCE(GREATEST(ps.sold_date,dsd.max_dc_sold_date),a.sold__c,a.date_of_sale__c,nd.sold_date,ahsd.sold_date::date)::date as sold_date,--GREATEST IS used because IN SOME cases there IS debt collection EVEN AFTER customer bought
   COALESCE(ps.sold_price,dsd.sold_price,nd.paid_amount,a.sell_price__c,a.sale_amount__c) as sold_price,
   CASE WHEN dsd.max_dc_sold_date IS NOT NULL THEN TRUE ELSE FALSE END is_sold_by_dc,
  ld.lost_date,
  wod.write_off_date,
  COALESCE(GREATEST(ps.sold_date,dsd.max_dc_sold_date),a.sold__c,a.date_of_sale__c,ahsd.sold_date,nd.sold_date,ld.lost_date, wod.write_off_date, 'now' :: text :: date :: timestamp) :: date - COALESCE(
    a.purchased__c,
    a.invoice_date__c :: timestamp,
    a.createddate
  ) :: date AS days_on_book,
  days_on_book / 30 + 1 AS months_on_book,
  selling_agent__c as selling_agent,
  coalesce(su.supplier_name, 'N/A') as supplier,
  coalesce(su.supplier_account,'N/A') as supplier_account,
  a.purchase_request__c as purchase_request_id,
  a.purchase_request_item__c as purchase_request_item_id,
  a.status as asset_status_original,
  CASE
    WHEN a.status :: text in (
      'ON LOAN' :: character varying :: text,
      'IN DEBT COLLECTION' :: character varying :: text,
      'IN DEBT COLLECTION' :: character varying :: text,
      'OFFICE' :: character varying :: text
    ) THEN 'ON RENT' :: text
    WHEN a.status :: text = 'IN STOCK' :: text THEN 'IN STOCK' :: text
    WHEN a2.status = 'LOST SOLVED' :: text
    OR a.status :: text = 'LOST SOLVED' :: text
    OR a.status :: text = 'RECOVERED' :: text
    THEN 'LOST SOLVED' :: text
    WHEN (
      a.status :: text in (
        'SOLD' :: character varying :: text,
        'SOLD TEST' :: character varying :: text
      )
    ) 
    THEN 'SOLD' :: text
    WHEN a.status :: text in (
      'RESERVED' :: character varying :: text,
      'SHIPPED' :: character varying :: text,
      'READY TO SHIP' :: character varying :: text,
      'TRANSFERRED TO WH' :: character varying :: text,
      'CROSS SALE' :: character varying :: text,
      'DROPSHIPPED' :: character varying :: text,
      'PURCHASED' :: character varying :: text
    ) THEN 'TRANSITION' :: text
    WHEN a.status :: text = 'SELLING' :: text THEN 'SELLING' :: text
    WHEN a.status :: text = 'LOST' :: text THEN 'LOST' :: text
    WHEN a.status :: text in (
      --'IN REPAIR' :: character varying :: text,
      'INCOMPLETE' :: character varying :: text,
      'RETURNED' :: character varying :: text,
      'LOCKED DEVICE' :: character varying :: text,
      'IN TRANSIT' :: character varying :: text,
      'WARRANTY' :: character varying :: text,
      --'IRREPARABLE' :: character varying :: text,
      'SEND FOR REFURBISHMENT' :: character varying :: text,
      'SENT FOR REFURBISHMENT' :: character varying :: text,
      'WAITING FOR REFURBISHMENT' :: character varying :: text
    ) THEN 'REFURBISHMENT' :: text
    WHEN a.status :: text = 'IN REPAIR' :: text THEN 'IN REPAIR' :: text
    WHEN a.status :: text in (
      'DELETED' :: character varying :: text,
      'BUY NOT APPROVED' :: character varying :: text,
      'BUY' :: character varying :: text,
      'RETURNED TO SUPPLIER' :: character varying :: text
    ) THEN 'NEVER PURCHASED' :: text
     WHEN a.status :: text = 'IRREPARABLE'
       THEN 'IRREPARABLE' :: text
      WHEN a.status :: text like '%WRITTEN OFF%'
      THEN 'WRITTEN OFF'
      ELSE 'NOT CLASSIFIED' :: text
  END AS asset_status_grouped,
  a.lost_reason__c as lost_reason,
  a.warehouse__c as warehouse,
  a.warehouse_refurbishment__c as wh_refurbishment_date,
  a.days_in_warehouse__c as days_in_warehouse,
  current_date - coalesce(isd._in_stock_at, a.createddate ) :: date as days_in_stock,
  a.revenue_share__c as revenue_share,
  a.capital_source__c as capital_source_sfid,
  coalesce(s.name,
  CASE 
     WHEN pri.capital_source_name IN ('Fidor Finco II') THEN 'Grover Finance II GmbH'
     WHEN pri.capital_source_name IN ('Commerz FinCo I','Fidor FinCo I','Commerzbank') THEN 'Grover Finance I GmbH'
     WHEN pri.capital_source_name ='Grover Group GmbH' THEN 'Grover Group GmbH'
     WHEN pri.capital_source_name ='USA Silicon Valley' THEN 'USA_test'
     ELSE pri.capital_source_name
  END) capital_source_name,
  a.order_number__c as asset_order_number,
  a.invoice_number__c as invoice_number,
  a.invoice_date__c as invoice_date,
  a.invoice_url__c as invoice_url,
  a.invoice_total__c as invoice_total,
  coalesce(a.final_condition__c, a.condition__c) AS asset_condition,
  CASE
    WHEN a.final_condition__c in (
      'DAMAGED',
      'INCOMPLETE',
      'IRREPARABLE/DISPOSAL',
      'LOCKED',
      'WARRANTY'
    ) then 'USED'
    WHEN (
      coalesce(a.final_condition__c, a.condition__c) = 'USED'
      OR (
        a.status in ('IN REPAIR', 'WARRANTY', 'IRREPARABLE')
      )
    ) THEN 'USED'
    WHEN coalesce(a.final_condition__c, a.condition__c) = 'NEW'
    AND COALESCE(d.delivered_allocations, 0) > 0 THEN 'AGAN'
    WHEN coalesce(a.final_condition__c, a.condition__c) = 'NEW' THEN 'NEW'
    WHEN coalesce(a.final_condition__c, a.condition__c) = 'AGAN' THEN 'AGAN'
    ELSE NULL
  END AS asset_condition_spv,
  a.asset_quality__c as asset_quality,
  a.condition_note__c as condition_note,
  a.external_condition_note__c as external_condition_note,
  a.external_condition__c as external_condition,
  a.functional_condition__c as functional_condition,
  a.package_condition__c as package_condition,
  a.final_condition__c as final_condition,
  a.number_of_rents__c as times_rented,
  a.debt_collection_not_recoverable__c as debt_collection_non_recoverable_date,
  a.createddate as created_date,
  greatest(
     a.lastmodifieddate,
     a.systemmodstamp,
     s.lastmodifieddate,
     s.systemmodstamp,
     su.last_modified_date,
     aa.lastmodifieddate,
     aa.systemmodstamp,
     cust.lastmodifieddate,
     cust.systemmodstamp,
     u.updated_at,
     p.updated_at,
     d.updated_at,
     a2.updated_at,
     v.variant_updated_at) as updated_date,
  CASE
    WHEN a.status :: text = 'OFFICE' :: text
    ELSE 'others' :: text
  END AS office_or_sponsorships,
  COALESCE(ls.country_name,cust.shippingcountry) as shipping_country,
   su.locale__c
from stg_salesforce.asset a
left join stg_salesforce.capital_source__c s on a.capital_source__c = s.id
left join ods_production.supplier su on su.supplier_id = a.supplier__c
left join stg_salesforce.customer_asset_allocation__c aa on aa.id = asset_allocation__c
left JOIN stg_salesforce.account CUST ON CUST.ID = a.accountid
LEFT JOIN asset_history_sold_date ahsd ON ahsd.asset_id=a.id
LEFT JOIN dc_cases dsd ON dsd.asset_id=a.id 
left join stg_api_production.spree_users u on u.id = CUST.spree_customer_id__c
LEFT JOIN dhl_lost a2 ON a2.asset_id::text = a.id::text
left join ods_production.product p on a.f_product_sku_product__c = p.product_sku
left join delivered_allocations d on a.id = d.asset_id
left join ods_production.variant v on a.f_product_sku_variant__c = v.variant_sku
left join payment_sold_date ps on ps.asset_id = a.id
LEFT JOIN new_infra_sold_Date nd ON nd.asset_id=a.id
LEFT JOIN write_off_date wod ON a.id = wod.assetid
LEFT JOIN lost_date ld ON a.id = ld.assetid
LEFT JOIN last_subscription ls ON ls.asset_id=a.id
left join in_stock_date isd on a.id = isd.asset_id
LEFT JOIN ods_production.purchase_request_item  pri ON a.purchase_request_item__c=pri.purchase_request_item_sfid
;


-- ad-hoc permission to recommerce group to avoid full access on ods_production
GRANT SELECT ON ods_production.asset TO GROUP recommerce,GROUP finance;
GRANT SELECT ON ods_production.asset to bart,anjali_savlani,hightouch_pricing,redash_pricing,airflow_recommerce;
GRANT SELECT ON ods_production.asset TO tableau;
GRANT SELECT ON ods_production.asset TO accounting_redash;
GRANT SELECT ON ods_production.asset TO GROUP mckinsey;
