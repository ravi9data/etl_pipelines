drop table if exists ods_production.payment_all;
create table ods_production.payment_all AS 
with a as (
         SELECT
         	s.subscription_payment_id AS payment_id,
            s.subscription_payment_name as payment_sfid,
            s.psp_reference_id AS psp_reference,
            s.customer_id::integer as customer_id,
            s.order_id AS order_id,
            s.subscription_id,
            s.asset_id,
            s.payment_type,
            s.status,
            s.updated_at,
            s.due_date,
            s.paid_date,
            s.failed_date,
            s.amount_due,
            s.amount_paid,
            s.payment_method,
            s.currency
           FROM ods_production.payment_subscription s
        UNION ALL
         SELECT 
            ap.asset_payment_id as payment_id,
         	ap.asset_payment_sfid AS payment_sfid,
           	ap.psp_reference_id AS psp_reference,
           	ap.customer_id::integer as customer_id,
            ap.order_id AS order_id,
            ap.subscription_id,
            ap.asset_id,
            ap.payment_type AS payment_type,
            ap.status AS status,
            ap.updated_at as updated_at,
            ap.due_date AS due_date,
            ap.paid_date AS paid_date,
            ap.failed_date AS failed_date,
            ap.amount_due AS amount_due,
            ap.amount_paid AS amount_paid,
            ap.payment_method AS payment_method,
  			ap.currency
           FROM ods_production.payment_asset ap
        UNION ALL
         SELECT 
            r.refund_payment_id as payment_Id,
         	r.refund_payment_sfid AS payment_sfid,
           	r.psp_reference_id AS psp_reference,
           	r.customer_id::integer as customer_id,
            r.order_id AS order_id,
            r.subscription_id,
            r.asset_id,
            r.refund_type||' '||related_payment_type AS payment_type,
            r.status AS status,
            r.paid_date AS due_date,
            r.updated_at as updated_at,
            r.paid_date AS paid_date,
            r.failed_date AS failed_date,
            '-1'::integer::double precision * r.amount_due AS amount_due,
            '-1'::integer::double precision * r.amount_refunded AS amount_paid,
            r.payment_method AS payment_method,
  r.currency
           FROM ods_production.payment_refund r
        )
        ,
final1 as ( SELECT 
 	a.payment_id,
    a.payment_sfid,
    a.psp_reference,
    a.customer_id,
    a.order_id,
    a.subscription_id,
    a.asset_id,
    asset.capital_source_name as capital_source,
    asset.purchased_date,
    a.payment_type,
    a.status,
    a.updated_at,
    a.due_date,
    a.paid_date,
    a.failed_date,
    a.amount_due,
    a.amount_paid,
    a.payment_method,
    a.currency
   FROM a
left join ods_production.asset asset 
 on asset.asset_id=a.asset_id
where not (a.payment_type='FIRST' and a.status in ('FAILED','FAILED FULLY'))
and a.status not in ('CANCELLED'))

,
sales_tax_rate as (select order_id,tax_rate AS sales_tax_rate,payment_type, 
row_number() over(partition by order_id order by created_at asc ) as idx 
from ods_production.payment_subscription)
,
deduping as (select * from sales_tax_rate where idx=1)

select u.*,str.sales_tax_rate 
from final1 u 
left join deduping str on u.order_id = str.order_id ;


GRANT SELECT ON ods_production.payment_all TO debt_management_redash;
GRANT SELECT ON ods_production.payment_all TO elene_tsintsadze;
GRANT SELECT ON ods_production.payment_all TO tableau;
GRANT SELECT ON ods_production.payment_all TO redash_commercial;
