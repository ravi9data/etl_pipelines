delete from dwh.collected_revenue_historical where report_date = current_date;
insert into dwh.collected_revenue_historical 
with sub as (
select 
sp.date,
sp.paid_date::date as fact_date,
coalesce(sp.country_name,'n/a') as country_name,
coalesce(sp.payment_method_detailed,'n/a') as payment_method,
coalesce(sp.currency,'n/a') as currency,
coalesce(sp.capital_source,'n/a') as capital_source,
coalesce(sp.customer_id::text,'n/a') as customer_id,
coalesce(sp.payment_id) as payment_id,
coalesce(sp.psp_reference) as psp_reference_id,
coalesce(sp.invoice_number,'n/a') as invoice_number,
coalesce(sp.invoice_date,sp.invoice_sent_date)::date as invoice_date,
coalesce(sp.invoice_url,'n/a') as invoice_url,
coalesce(s.category_name,'n/a') as category_name,
coalesce(s.store_label,'n/a') as store_label, 
sum(sp.amount_paid) AS collected_revenue,
sum(
CASE
 WHEN sp.subscription_payment_category = 'PAID_TIMELY'::text 
  THEN coalesce(sp.amount_paid,0)-coalesce(sp.amount_shipment,0)
   ELSE 0::numeric
  END) AS collected_revenue_timely,
 sum(
CASE
 WHEN sp.subscription_payment_category in ('PAID_REFUNDED'::text,'PAID_CHARGEBACK'::text)
  THEN coalesce(sp.amount_paid,0)-coalesce(sp.amount_shipment,0)
   ELSE 0::numeric
  END) AS collected_revenue_refunded,
              sum(
                CASE
                    WHEN sp.subscription_payment_category = 'RECOVERY_ARREARS'::text 
                     THEN coalesce(sp.amount_paid,0)-coalesce(sp.amount_shipment,0)
                    ELSE 0::numeric
                END) AS collected_revenue_arrears,
            sum(
                CASE
                    WHEN sp.subscription_payment_category in 
                     ('RECOVERY_DELINQUENT_EARLY','RECOVERY_DELINQUENT_LATE') 
                      THEN coalesce(sp.amount_paid,0)-coalesce(sp.amount_shipment,0)
                    ELSE 0::numeric
                END) AS collected_revenue_delinquent,
            sum(
                CASE
                    WHEN sp.subscription_payment_category = 'RECOVERY_DEFAULT'::text 
                     THEN coalesce(sp.amount_paid,0)-coalesce(sp.amount_shipment,0)
                    ELSE 0::numeric
                END) AS collected_revenue_default,
            sum(
                CASE
                    WHEN sp.subscription_payment_category in ('PAID_TIMELY','PAID_REFUNDED','PAID_CHARGEBACK',
                    'RECOVERY_ARREARS','RECOVERY_DELINQUENT_EARLY','RECOVERY_DELINQUENT_LATE',
                    'RECOVERY_DEFAULT')
                     THEN sp.amount_shipment
                    ELSE 0::numeric
                END) AS collected_amount_shipment_sp,
avg(
	case 
	when sp.country_name = 'Austria' and sp.due_date >= '2022-04-01' then 0.20
	when sp.country_name = 'Netherlands' and sp.due_date >= '2022-04-01' then 0.21
	when sp.country_name = 'Spain' and sp.due_date >= '2022-04-01' then 0.21
	when sp.currency = 'USD' then coalesce(sp.sales_tax_rate,sp.vat_tax_rate)
	else sp.vat_tax_rate end
	) as vat_rate
from master.subscription_payment_historical sp
left join master.subscription_historical s 
 on sp.subscription_id=s.subscription_id
 and sp.date=s.date
where sp.paid_date::date <= current_date-1
and sp.date = current_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
order by 2 desc
)
,asset as (
select 
pa.date,
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
coalesce(s.store_label,'n/a') as store_label,
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
from master.asset_payment_historical pa 
left join master.asset_historical a 
 on a.asset_id=pa.asset_id
 and a.date = pa.date
left join master.subscription_historical s 
 on pa.subscription_id=s.subscription_id
 and s.date = pa.date
left join ods_production.store st 
 on st.id=s.store_id
left join master.order_historical o 
 on pa.order_id=o.order_id
 and pa.date = o.date
left join ods_production.store st2
 on st2.id=o.store_id
where pa.paid_date::date <= current_date-1
and pa.date = current_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
order by 2 desc 
)
,refunds as (
select distinct
r.date,
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
coalesce(s.store_label,'n/a') as store_label,
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
from master.refund_payment_historical r 
left join master.asset_payment_historical pa 
 on pa.asset_payment_id=r.asset_payment_id
 and pa.date = r.date
left join master.asset_historical a 
 on a.asset_id=pa.asset_id
 and a.date = pa.date
left join master.subscription_payment_historical ps 
 on ps.payment_id =r.subscription_payment_id
 and ps."date" = r."date" 
left join master.subscription_historical s 
 on pa.subscription_id=s.subscription_id
 and pa."date" = s."date" 
left join ods_production.store st 
 on st.id=s.store_id
left join master.order_historical o 
 on pa.order_id=o.order_id
 and pa.date= o.date
left join ods_production.store st2
 on st2.id=o.store_id
left join master.subscription_historical s2 
 on ps.subscription_id=s2.subscription_id
 and ps."date" = s2."date" 
left join ods_production.store st3 
 on st3.id=s2.store_id
where r.paid_date::date <= current_date-1
and r.paid_date::date is not null
and r.date = current_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
order by 2 desc 
)
 SELECT DISTINCT 
    COALESCE(s.date, a.date, r.date) AS report_date,
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
    coalesce(ch.customer_type,'normal_customer') as customer_type,
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
    coalesce(s.vat_rate,a.vat_rate,r.vat_rate) as vat_rate,
    COALESCE(s.store_label,a.store_label, r.store_label) AS store_label
   FROM sub s
   FULL JOIN asset a 
      ON s.fact_date = a.fact_date 
      and s.date = a.date
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
     and r.date = s.date
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
     left join master.customer_historical ch 
     on coalesce(ch.customer_id::text,'n/a')=COALESCE(s.customer_id, a.customer_id,r.customer_id,'n/a')
     and ch."date" = COALESCE(s.date,a.date, r.date)
    left join ods_data_sensitive.customer_pii pii 
     on coalesce(pii.customer_id::text,'n/a')=COALESCE(s.customer_id,a.customer_id,r.customer_id,'n/a')
     ;
