drop table if exists dwh.collected_revenue;
create table dwh.collected_revenue as 
with sub as (
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
sum(sp.amount_paid) AS collected_revenue,
sum(
CASE
 WHEN d.subscription_payment_category = 'PAID_TIMELY'::text 
  THEN coalesce(sp.amount_paid,0)-coalesce(sp.amount_shipment,0)
   ELSE 0::numeric
  END) AS collected_revenue_timely,
 sum(
CASE
 WHEN d.subscription_payment_category in ('PAID_REFUNDED'::text,'PAID_CHARGEBACK'::text)
  THEN coalesce(sp.amount_paid,0)-coalesce(sp.amount_shipment,0)
   ELSE 0::numeric
  END) AS collected_revenue_refunded,
              sum(
                CASE
                    WHEN d.subscription_payment_category = 'RECOVERY_ARREARS'::text 
                     THEN coalesce(sp.amount_paid,0)-coalesce(sp.amount_shipment,0)
                    ELSE 0::numeric
                END) AS collected_revenue_arrears,
            sum(
                CASE
                    WHEN d.subscription_payment_category in 
                     ('RECOVERY_DELINQUENT_EARLY','RECOVERY_DELINQUENT_LATE') 
                      THEN coalesce(sp.amount_paid,0)-coalesce(sp.amount_shipment,0)
                    ELSE 0::numeric
                END) AS collected_revenue_delinquent,
            sum(
                CASE
                    WHEN d.subscription_payment_category = 'RECOVERY_DEFAULT'::text 
                     THEN coalesce(sp.amount_paid,0)-coalesce(sp.amount_shipment,0)
                    ELSE 0::numeric
                END) AS collected_revenue_default,
            sum(
                CASE
                    WHEN d.subscription_payment_category in ('PAID_TIMELY','PAID_REFUNDED','PAID_CHARGEBACK',
                    'RECOVERY_ARREARS','RECOVERY_DELINQUENT_EARLY','RECOVERY_DELINQUENT_LATE',
                    'RECOVERY_DEFAULT')
                     THEN sp.amount_shipment
                    ELSE 0::numeric
                END) AS collected_amount_shipment_sp,
avg(
	case 
	when s.country_name = 'Austria' and sp.due_date >= '2022-04-01' then 0.20
	when s.country_name = 'Netherlands' and sp.due_date >= '2022-04-01' then 0.21
	when s.country_name = 'Spain' and sp.due_date >= '2022-04-01' then 0.21
	when sp.currency = 'USD' then coalesce(sp.sales_tax_rate,sp.vat_tax_rate)
	else sp.vat_tax_rate end
) as vat_rate
from ods_production.payment_subscription sp
left join ods_production.payment_subscription_details d 
 on sp.subscription_payment_id=d.subscription_payment_id
 and coalesce(sp.paid_date,'1990-05-22')=coalesce(d.paid_date,'1990-05-22')
left join ods_production.subscription s 
 on sp.subscription_id=s.subscription_id
left join ods_production.store st 
 on st.id=s.store_id
where sp.paid_date::date <= current_date-1
group by 1,2,3,4,5,6,7,8,9,10,11,12
order by 1 desc
)
,asset as (
select 
pa.paid_date::date as fact_date,
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
        sum(
                CASE
                    WHEN pa.payment_type::text = 'REPAIR COST'::text 
                    THEN pa.amount_paid
                    ELSE 0
                END) AS repair_cost_collected_revenue,
         sum(
                CASE
                    WHEN pa.payment_type in ('CUSTOMER BOUGHT', 'GROVER SOLD') 
                    THEN pa.amount_paid
                    ELSE 0::numeric
                END) AS asset_sales_collected_revenue,
        sum(
                CASE
                    WHEN pa.payment_type in ('ADDITIONAL CHARGE' ,'COMPENSATION')
                    THEN pa.amount_paid
                    ELSE 0::numeric
                END) AS additional_charge_collected_revenue,
          sum(
                CASE
                    WHEN pa.payment_type in ('DEBT COLLECTION')
                    THEN pa.amount_paid
                    ELSE 0::numeric
                END) AS debt_collection_collected_revenue,
        sum(
                CASE
                    WHEN pa.payment_type = 'SHIPMENT' 
                    THEN pa.amount_paid
                    ELSE 0::numeric
                END) AS collected_amount_shipment_ap,
                avg(pa.vat_rate) as vat_rate
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
group by 1,2,3,4,5,6,7,8,9,10,11,12
order by 1 desc 
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
sum(case when r.status = 'PAID' 
		and r.refund_type= 'REFUND' 
		 then -1*r.amount_refunded 
		else 0 end) as amount_refund_paid,
sum(case when r.status = 'PAID' 
 		and r.refund_type= 'CHARGE BACK' 
 		then -1*r.amount_refunded 
 	else 0 end) as amount_chb_paid,
sum(case when r.status = 'PAID' 
		and refund_type= 'REFUND' 
		AND RELATED_PAYMENT_TYPE = 'SUBSCRIPTION_PAYMENT' 
		then -1*amount_refunded 
		else 0 end) as amount_refund_paid_SUB_PAYMENT,
sum(case when r.status = 'PAID' 
		and refund_type= 'CHARGE BACK' 
		AND RELATED_PAYMENT_TYPE = 'SUBSCRIPTION_PAYMENT' 
		then -1*amount_refunded 
		else 0 end) as amount_chb_paid_SUB_PAYMENT,
sum(case when r.status = 'PAID' 
		and refund_type= 'REFUND' 
		AND RELATED_PAYMENT_TYPE = 'ASSET_PAYMENT' 
		then -1*amount_refunded 
		else 0 end) as amount_refund_paid_ASSET_PAYMENT,
sum(case when r.status = 'PAID' 
		and refund_type= 'CHARGE BACK' 
		AND RELATED_PAYMENT_TYPE = 'ASSET_PAYMENT' 
		then -1*amount_refunded 
		else 0 end) as amount_chb_paid_ASSET_PAYMENT,
		avg(r.vat_rate) as vat_rate
from ods_production.payment_refund r 
left join ods_production.payment_asset pa 
 on pa.asset_payment_id=r.asset_payment_id
left join ods_production.asset a 
 on a.asset_Id=pa.asset_Id
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
group by 1,2,3,4,5,6,7,8,9,10,11,12
order by 1 desc 
)
 SELECT DISTINCT 
    COALESCE(s.fact_date, a.fact_date,r.fact_date) AS fact_date,
    coalesce(s.country_name, a.country_name,r.country_name) AS country_name,
    COALESCE(s.currency, a.currency, r.currency) AS currency,
    COALESCE(s.capital_source,a.capital_source,r.capital_source) AS capital_source,
    COALESCE(s.payment_method,a.payment_method,r.payment_method) AS payment_method,
    COALESCE(s.customer_id,a.customer_id,r.customer_id) AS customer_id,
    COALESCE(s.category_name,a.category_name,r.category_name) AS category_name,
    COALESCE(s.payment_id,a.payment_id,r.payment_id) AS payment_id,
    COALESCE(s.psp_reference_id,a.psp_reference_id,r.psp_reference_id) AS psp_reference_id,
    COALESCE(s.invoice_number,a.invoice_number,r.invoice_number) AS invoice_number,
    COALESCE(s.invoice_date,a.invoice_date,r.invoice_date) AS invoice_date,
    COALESCE(s.invoice_url,a.invoice_url,r.invoice_url) AS invoice_url,
    coalesce(pii.customer_type,'normal_customer') as customer_type,
    coalesce(pii.tax_id,'n/a') as tax_id,
    coalesce(s.collected_revenue,0) as collected_revenue,
    coalesce(s.collected_revenue_refunded,0) as collected_revenue_refunded,
    coalesce(s.collected_revenue_timely,0) as collected_revenue_timely,
    coalesce(s.collected_revenue_arrears,0) as collected_revenue_arrears,
    coalesce(s.collected_revenue_delinquent,0) as collected_revenue_delinquent,
    coalesce(s.collected_revenue_default,0) as collected_revenue_default,
    coalesce(s.collected_amount_shipment_sp,0)+coalesce(collected_amount_shipment_ap,0) as collected_revenue_shipment,
    coalesce(a.repair_cost_collected_revenue,0) as repair_cost_collected_revenue,
    coalesce(a.asset_sales_collected_revenue,0) as asset_sales_collected_revenue,
    coalesce(a.additional_charge_collected_revenue,0) as additional_charge_collected_revenue,
    coalesce(a.debt_collection_collected_revenue,0) as debt_collection_collected_revenue,
    coalesce(amount_refund_paid,0) as amount_refund_paid,
    coalesce(amount_chb_paid,0) as amount_chb_paid,
    coalesce(amount_refund_paid_SUB_PAYMENT,0) as amount_refund_paid_SUB_PAYMENT,
    coalesce(amount_chb_paid_SUB_PAYMENT,0) as amount_chb_paid_SUB_PAYMENT,
    coalesce(amount_refund_paid_ASSET_PAYMENT,0) as amount_refund_paid_ASSET_PAYMENT,
    coalesce(amount_chb_paid_ASSET_PAYMENT,0) as amount_chb_paid_ASSET_PAYMENT,
    coalesce(s.vat_rate,a.vat_rate,r.vat_rate) as vat_rate
   FROM sub s
   FULL JOIN asset a 
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
    left join ods_data_sensitive.customer_pii pii 
     on coalesce(pii.customer_id::text,'n/a')=COALESCE(s.customer_id,a.customer_id,r.customer_id,'n/a')
     order by 1 desc;

GRANT SELECT ON dwh.collected_revenue TO tableau;
