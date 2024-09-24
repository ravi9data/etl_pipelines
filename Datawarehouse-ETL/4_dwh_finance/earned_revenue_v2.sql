drop table if exists dwh.earned_revenue_v2;
create table dwh.earned_revenue_v2 as 
with sub as (
select 
sp.billing_period_start::date as fact_date,
coalesce(st.country_name,'n/a') as country_name,
coalesce(sp.payment_method,'n/a') as payment_method,
coalesce(sp.currency,'n/a') as currency,
coalesce(sp.capital_source,'n/a') as capital_source,
coalesce(sp.customer_id::text,'n/a') as customer_id,
coalesce(sp.subscription_payment_id) as payment_id,
coalesce(sp.psp_reference_id) as psp_reference_id,
coalesce(sp.invoice_number,'n/a') as invoice_number,
coalesce(sp.invoice_date,sp.invoice_sent_date)::date as invoice_date,
coalesce(sp.invoice_url,'n/a') as invoice_url,
coalesce(s.category_name,'n/a') as category_name, 
coalesce(s.subcategory_name,'n/a') as subcategory_name, 
sum(sp.amount_paid) AS collected_revenue,
avg(
   case 
   when s.country_name = 'Austria' and sp.due_date >= '2022-04-01' then 0.20
   when s.country_name = 'Netherlands' and sp.due_date >= '2022-04-01' then 0.21
   when s.country_name = 'Spain' and sp.due_date >= '2022-04-01' then 0.21
   when sp.currency = 'USD' then sp.tax_rate
   else sp.tax_rate end
) as vat_rate,
   sum(sp.amount_due) as earned_revenue_incl_vat,
   sum(sp.amount_subscription) as amount_subscription,
   sum(sp.amount_shipment) as amount_shipment_sp,
   sum(COALESCE(sp.amount_voucher, 0::numeric) + 
   COALESCE(sp.amount_discount, 0::numeric)) AS amount_discount
from ods_production.payment_subscription sp
left join ods_production.subscription s 
 on sp.subscription_id=s.subscription_id
left join ods_production.store st 
 on st.id=s.store_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
) 
,overdue_fee as (
select 
sp.paid_date::date as fact_date,
coalesce(st.country_name,'n/a') as country_name,
coalesce(sp.payment_method,'n/a') as payment_method,
coalesce(sp.currency,'n/a') as currency,
coalesce(sp.capital_source,'n/a') as capital_source,
coalesce(sp.customer_id::text,'n/a') as customer_id,
coalesce(sp.subscription_payment_id) as payment_id,
coalesce(sp.psp_reference_id) as psp_reference_id,
coalesce(sp.invoice_number,'n/a') as invoice_number,
coalesce(sp.invoice_date,sp.invoice_sent_date)::date as invoice_date,
coalesce(sp.invoice_url,'n/a') as invoice_url,
coalesce(s.category_name,'n/a') as category_name, 
coalesce(s.subcategory_name,'n/a') as subcategory_name,
sum(sp.amount_paid) AS collected_revenue,
avg(
   case 
   when s.country_name = 'Austria' and sp.due_date >= '2022-04-01' then 0.20
   when s.country_name = 'Netherlands' and sp.due_date >= '2022-04-01' then 0.21
   when s.country_name = 'Spain' and sp.due_date >= '2022-04-01' then 0.21
   when sp.currency = 'USD' then sp.tax_rate
   else sp.tax_rate end
) as vat_rate,
coalesce(sum(sp.amount_overdue_fee),0) as amount_overdue_fee
from ods_production.payment_subscription sp
left join ods_production.subscription s 
 on sp.subscription_id=s.subscription_id
left join ods_production.store st 
 on st.id=s.store_id
where sp.paid_date::date <= current_date-1
  and sp.paid_date::date is not null 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
,refunds as (
select distinct
r.paid_date::date as fact_date,
coalesce(st.country_name,st2.country_name,st3.country_name,'n/a') as country_name,
coalesce(r.payment_method,'n/a') as payment_method,
coalesce(r.currency,'n/a') as currency,
coalesce(r.capital_source,'n/a') as capital_source,
coalesce(r.customer_id::text,'n/a') as customer_id,
coalesce(r.refund_payment_id) as payment_id,
coalesce(r.psp_reference_id) as psp_reference_id,
coalesce(ps.invoice_number,pa.invoice_number,'n/a') as invoice_number,
coalesce(ps.invoice_date,ps.invoice_sent_date,pa.invoice_date,pa.invoice_sent_date)::date as invoice_date,
coalesce(ps.invoice_url,pa.invoice_url,'n/a') as invoice_url,
coalesce(s.category_name,s2.category_name,a.category_name,'n/a') as category_name,
coalesce(s.subcategory_name,s2.subcategory_name,a.subcategory_name,'n/a') as subcategory_name, 
sum(case when r.status = 'PAID' and refund_type= 'REFUND' then -1*amount_refunded else 0 end) as amount_refund_paid,
sum(case when r.status = 'PAID' and refund_type= 'CHARGE BACK' then amount_refunded else 0 end) as amount_chb_paid,
sum(case when r.status = 'PAID' and refund_type= 'REFUND' 
 AND RELATED_PAYMENT_TYPE = 'SUBSCRIPTION_PAYMENT'  then -1*amount_refunded else 0 end) as amount_refund_paid_SUB_PAYMENT,
 sum(case when r.status = 'PAID' and refund_type= 'REFUND' 
 AND  related_payment_type_detailed='SHIPMENT' then -1*amount_refunded else 0 end) as amount_refund_paid_shipment_payment,
sum(case when r.status = 'PAID' and refund_type= 'CHARGE BACK' 
 AND (RELATED_PAYMENT_TYPE = 'SUBSCRIPTION_PAYMENT' or related_payment_type_detailed='SHIPMENT') then -1*amount_refunded else 0 end) as amount_chb_paid_SUB_PAYMENT,
sum(case when r.status = 'PAID' and refund_type= 'REFUND' 
 AND RELATED_PAYMENT_TYPE = 'ASSET_PAYMENT' and related_payment_type_detailed not in ('SHIPMENT') then -1*amount_refunded else 0 end) as amount_refund_paid_ASSET_PAYMENT,
sum(case when r.status = 'PAID' and refund_type= 'CHARGE BACK' 
 AND RELATED_PAYMENT_TYPE = 'ASSET_PAYMENT' and related_payment_type_detailed not in ('SHIPMENT') then -1*amount_refunded else 0 end) as amount_chb_paid_ASSET_PAYMENT,
avg(coalesce(r.tax_rate,ps.tax_rate,
(case  
 when pa.payment_type in ('COMPENSATION') 
  then 0
when coalesce(st.country_name,st2.country_name,'n/a') in ('Austria')
 and pa.payment_type in ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
  then 0.2
when coalesce(st.country_name,st2.country_name,'n/a') in ('Netherlands')
 and pa.payment_type in ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
  then 0.21
when coalesce(st.country_name,st2.country_name,'n/a') in ('Spain')
 and pa.payment_type in ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
  then 0.21
when r.paid_date::date between '2020-07-01' and '2020-12-31'
   then 0.16
when r.paid_date::date < '2020-07-01'
   then 0.19
when r.paid_date::date >= '2021-01-01'
 then 0.19
when pa.paid_date::date between '2020-07-01' and '2020-12-31'
   then 0.16
when pa.paid_date::date < '2020-07-01'
   then 0.19
when pa.paid_date::date >= '2021-01-01'
 then 0.19
end) 
)) as vat_rate
from ods_production.payment_refund r 
left join ods_production.payment_asset pa 
 on pa.asset_payment_id=r.asset_payment_id
left join ods_production.asset a 
 on a.asset_id=pa.asset_id 
left join ods_production.payment_subscription ps 
 on ps.subscription_payment_id=r.subscription_payment_id
left join ods_production.subscription s 
 on pa.subscription_id=s.subscription_id
left join ods_production.store st 
 on st.id=s.store_id
left join ods_production.order o 
 on pa.order_id=o.order_id
left join ods_production.store st2
 on st2.id=o.store_id
left join ods_production.subscription s2 
 on ps.subscription_id=s2.subscription_id
left join ods_production.store st3 
 on st3.id=s2.store_id
where r.paid_date::date <= current_date-1
and r.paid_date::date is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
,asset_due as (
select 
pa.due_date::date as fact_date,
coalesce(st.country_name,st2.country_name,'n/a') as country_name,
coalesce(pa.payment_method,'n/a') as payment_method,
coalesce(pa.currency,'n/a') as currency,
coalesce(pa.capital_source,'n/a') as capital_source,
coalesce(pa.customer_id::text,'n/a') as customer_id,
coalesce(pa.asset_payment_id) as payment_id,
coalesce(pa.psp_reference_id) as psp_reference_id,
coalesce(pa.invoice_number,'n/a') as invoice_number,
coalesce(pa.invoice_date,pa.invoice_sent_date)::date as invoice_date,
coalesce(pa.invoice_url,'n/a') as invoice_url,
coalesce(s.category_name,a.category_name,'n/a') as category_name, 
coalesce(s.subcategory_name,a.subcategory_name,'n/a') as subcategory_name, 
sum(
                CASE
                    WHEN pa.payment_type = 'SHIPMENT' 
                    THEN pa.amount_due
                    ELSE 0::numeric
 END) AS amount_shipment_ap,
avg(coalesce(pa.tax_rate,
(case  
 when pa.payment_type in ('COMPENSATION') 
  then 0
when coalesce(st.country_name,st2.country_name,'n/a') in ('Austria')
 and pa.payment_type in ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
  then 0.2
when coalesce(st.country_name,st2.country_name,'n/a') in ('Netherlands')
 and pa.payment_type in ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
  then 0.21
when coalesce(st.country_name,st2.country_name,'n/a') in ('Spain')
 and pa.payment_type in ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
  then 0.21  
when pa.paid_date::date between '2020-07-01' and '2020-12-31'
   then 0.16
when pa.paid_date::date < '2020-07-01'
   then 0.19
when pa.paid_date::date >= '2021-01-01'
 then 0.19
end) 
)) as vat_rate
from ods_production.payment_asset pa 
left join ods_production.asset a 
 on a.asset_id=pa.asset_id
left join ods_production.subscription s 
 on pa.subscription_id=s.subscription_id
left join ods_production.store st 
 on st.id=s.store_id
left join ods_production.order o 
 on pa.order_id=o.order_id
left join ods_production.store st2
 on st2.id=o.store_id
where pa.paid_date::date <= current_date-1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
,asset_paid as (
select 
pa.paid_date::date as fact_date,
case 
   when pa.payment_type in ('GROVER SOLD') then 'Germany' 
   else coalesce(st.country_name,st2.country_name,'n/a')
end as country_name,
coalesce(pa.payment_method,'n/a') as payment_method,
coalesce(pa.currency,'n/a') as currency,
coalesce(pa.capital_source,'n/a') as capital_source,
coalesce(pa.customer_id::text,'n/a') as customer_id,
coalesce(pa.asset_payment_id) as payment_id,
coalesce(pa.psp_reference_id) as psp_reference_id,
coalesce(pa.invoice_number,'n/a') as invoice_number,
coalesce(pa.invoice_date,pa.invoice_sent_date)::date as invoice_date,
coalesce(pa.invoice_url,'n/a') as invoice_url,
coalesce(s.category_name,a.category_name,'n/a') as category_name, 
coalesce(s.subcategory_name,a.subcategory_name,'n/a') as subcategory_name,
        sum(
                CASE
                    WHEN pa.payment_type::text = 'REPAIR COST'::text 
                    THEN pa.amount_paid
                    ELSE 0
                END) AS repair_cost_collected_revenue,
         sum(
                CASE
                    WHEN pa.payment_type in ('CUSTOMER BOUGHT') 
                    THEN pa.amount_paid
                    ELSE 0::numeric
                END) AS asset_sales_customer_bought_collected_revenue,
        sum(
                CASE
                    WHEN pa.payment_type in ('GROVER SOLD') 
                    THEN pa.amount_paid
                    ELSE 0::numeric
        sum(
                CASE
                    WHEN pa.payment_type in ('ADDITIONAL CHARGE')
                    THEN pa.amount_paid
                    ELSE 0::numeric
                END) AS additional_charge_collected_revenue,
   sum(
                CASE
                    WHEN pa.payment_type in ('COMPENSATION')
                    THEN pa.amount_paid
                    ELSE 0::numeric
                END) AS compensation_collected_revenue,
avg(
case  
 when payment_type in ('COMPENSATION') 
  then 0
when coalesce(st.country_name,st2.country_name,'n/a') in ('Austria')
 and pa.payment_type in ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
  then 0.2
when coalesce(st.country_name,st2.country_name,'n/a') in ('Netherlands')
 and pa.payment_type in ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
  then 0.21
when coalesce(st.country_name,st2.country_name,'n/a') in ('Spain')
 and pa.payment_type in ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
  then 0.21  
when pa.paid_date::date between '2020-07-01' and '2020-12-31'
   then 0.16
when pa.paid_date::date < '2020-07-01'
   then 0.19
when pa.paid_date::date >= '2021-01-01'
   then 0.19
end) as vat_rate
from ods_production.payment_asset pa 
left join ods_production.asset a 
 on a.asset_id=pa.asset_id
left join ods_production.subscription s 
 on pa.subscription_id=s.subscription_id
left join ods_production.store st 
 on st.id=s.store_id
left join ods_production.order o 
 on pa.order_id=o.order_id
left join ods_production.store st2
 on st2.id=o.store_id
where pa.paid_date::date <= current_date-1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
select 
    COALESCE(s.fact_date, a2.fact_date, a.fact_date, r.fact_Date, o.fact_date) AS fact_date,
    coalesce(s.country_name, a2.country_name,a.country_name,r.country_name,o.country_name,'n/a') AS country_name,
    coalesce(s.currency, a2.currency,a.currency,r.currency,o.currency,'n/a') AS currency,
    coalesce(s.capital_source, a2.capital_source,a.capital_source,r.capital_source,o.capital_source,'n/a') AS capital_source,
    coalesce(s.category_name, a2.category_name,a.category_name,r.category_name,o.category_name,'n/a') AS category_name,
    coalesce(s.subcategory_name, a2.subcategory_name,a.subcategory_name,r.subcategory_name,o.subcategory_name,'n/a') AS subcategory_name,
    coalesce(s.payment_method, a2.payment_method,a.payment_method,r.payment_method,o.payment_method,'n/a') AS payment_method,
    coalesce(s.customer_id, a2.customer_id,a.customer_id,r.customer_id,o.customer_id,'n/a') AS customer_id,
    coalesce(s.payment_id, a2.payment_id,a.payment_id,r.payment_id,o.payment_id,'n/a') AS payment_id,
    coalesce(s.psp_reference_id, a2.psp_reference_id,a.psp_reference_id,r.psp_reference_id,o.psp_reference_id,'n/a') AS psp_reference_id,
    coalesce(s.invoice_number, a2.invoice_number,a.invoice_number,r.invoice_number,o.invoice_number,'n/a') AS invoice_number,
    coalesce(s.invoice_date, a2.invoice_date,a.invoice_date,r.invoice_date,o.invoice_date) AS invoice_date,
    coalesce(s.invoice_url, a2.invoice_url,a.invoice_url,r.invoice_url,o.invoice_url,'n/a') AS invoice_url,
    coalesce(pii.customer_type,'normal_customer') as customer_type,
    coalesce(pii.company_name,'normal_customer') as company_name,
    coalesce(pii.tax_id,'n/a') as tax_id,
    coalesce(earned_revenue_incl_vat,0) as earned_sub_revenue_incl_vat,
    coalesce(amount_subscription,0) as amount_subscription,
    coalesce(amount_shipment_sp,0)+coalesce(a.amount_shipment_ap,0) as amount_shipment,
    coalesce(amount_discount,0) as amount_discount,
    coalesce(amount_overdue_fee,0) as amount_overdue_fee,
    coalesce(a2.repair_cost_collected_revenue,0) as repair_cost_earned_revenue,
    coalesce(a2.asset_sales_customer_bought_collected_revenue,0) as asset_sales_customer_bought_earned_revenue,
    coalesce(a2.additional_charge_collected_revenue,0) as additional_charge_earned_revenue,
    coalesce(a2.compensation_collected_revenue,0) as compensation_earned_revenue,
    coalesce(amount_refund_paid,0) as refund_at_fact_Date,
    coalesce(amount_refund_paid_SUB_PAYMENT,0) as refund_sub_payment_at_fact_Date,
    coalesce(amount_refund_paid_shipment_payment,0) as refund_shipment_payment_at_fact_Date,
    coalesce(amount_refund_paid_ASSET_PAYMENT,0) as refund_asset_payment_at_fact_Date,
    coalesce(amount_chb_paid_SUB_PAYMENT,0) as chb_SUB_PAYMENT_at_fact_date,
    coalesce(amount_chb_paid_ASSET_PAYMENT,0) as chb_ASSET_PAYMENT_at_fact_date,
    coalesce(s.vat_rate,a.vat_rate,a2.vat_rate,r.vat_rate,o.vat_rate) as vat_rate
  FROM sub s
     FULL JOIN asset_due a 
      ON s.fact_date = a.fact_date 
      AND s.currency::text = a.currency::text 
      AND s.capital_source::text = a.capital_source::text
      and s.payment_method=a.payment_method
      and s.customer_id=a.customer_id
      and s.payment_id=a.payment_id
      and s.invoice_number=a.invoice_number
      and s.invoice_date=a.invoice_Date
      and s.invoice_url=a.invoice_url
      and s.country_name=a.country_name
      and s.psp_reference_id=a.psp_reference_id
      and s.category_name=a.category_name
      AND s.subcategory_name=a.subcategory_name
     FULL JOIN asset_paid a2 
      ON s.fact_date = a2.fact_date 
      AND s.currency::text = a2.currency::text 
      AND s.capital_source::text = a2.capital_source::text
      and s.payment_method=a2.payment_method
      and s.customer_id=a2.customer_id
      and s.payment_id=a2.payment_id
      and s.invoice_number=a2.invoice_number
      and s.invoice_date=a2.invoice_Date
      and s.invoice_url=a2.invoice_url
      and s.country_name=a2.country_name
      and s.psp_reference_id=a2.psp_reference_id
      and s.category_name=a2.category_name
      AND s.subcategory_name=a2.subcategory_name
     FULL JOIN refunds r 
     ON r.fact_date = s.fact_date 
      AND r.currency = s.currency::text 
      AND r.capital_source::text = s.capital_source::text
      and r.payment_method=s.payment_method
      and r.customer_id=s.customer_id
      and s.payment_id=r.payment_id
      and s.invoice_number=r.invoice_number
      and s.invoice_date=r.invoice_Date
      and s.invoice_url=r.invoice_url
      and s.country_name=r.country_name
      and s.psp_reference_id=r.psp_reference_id
      and s.category_name=r.category_name
      AND s.subcategory_name=r.subcategory_name
     FULL JOIN overdue_fee o 
     ON r.fact_date = o.fact_date 
      AND r.currency = o.currency::text 
      AND r.capital_source::text = o.capital_source::text
      and r.payment_method=o.payment_method
      and r.customer_id=o.customer_id
      and s.payment_id=o.payment_id
      and s.invoice_number=o.invoice_number
      and s.invoice_date=o.invoice_Date
      and s.invoice_url=o.invoice_url
      and s.country_name=o.country_name
      and s.psp_reference_id=o.psp_reference_id
      and s.category_name=o.category_name
      AND s.subcategory_name=o.subcategory_name
    left join ods_data_sensitive.customer_pii pii 
     on coalesce(pii.customer_id::text,'n/a')=COALESCE(a.customer_id, a2.customer_id,s.customer_id, r.customer_id, o.customer_id,'n/a')
     ;

GRANT SELECT ON dwh.earned_revenue_v2 TO tableau;
