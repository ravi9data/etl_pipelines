WITH card_customer AS (
SELECT
	customer_id,
	first_card_created_date ,
	first_timestamp_payment_successful 
WHERE first_card_created_date IS NOT NULL )
select distinct 
c.date as reporting_Date, 
c.customer_acquisition_cohort::date as customer_acquisition_cohort,
s2.start_date::date as first_due_date,
cc.first_timestamp_payment_successful ,
cc.first_card_created_date,
s.first_asset_delivery_date ,
s2.store_short as customer_acquisition_store,
s2.store_label as customer_acquisition_store_label,
s2.country_name as customer_acquisition_country,
s2.category_name as customer_acquisition_category_name,
s2.rental_period as acquired_subscription_plan,
c.customer_id,
p.order_id,
s.subscription_id,
c.customer_type,
coalesce(s.cancellation_reason_churn,'active') as cancellation_reason_churn,
o.status as order_status,
o.new_recurring,
p.payment_id,
p.status as payment_status,
p.payment_type,
p.due_date,
p.paid_date,
p.amount_due,
p.amount_paid,
p.tax_rate,
p.discount_amount as discount_amount,
greatest(p.due_date::date-c.customer_acquisition_cohort::date,0) as days_between_acquisition_due,
greatest(datediff(month, c.customer_acquisition_cohort::date, p.due_date::date),0) as months_between_acquisition_due,
greatest(p.paid_date::date-c.customer_acquisition_cohort::date,0) as days_between_acquisition_paid,
s.subscription_value,
(s.committed_sub_value + COALESCE(s.additional_committed_sub_value,0)) as committed_sub_value,
s.avg_asset_purchase_price,
s.subscription_plan,
/*COPY FROM ASSET CURVES
 * DUPLICATED LOGIC
 * */
(case when payment_type in ('FIRST','RECURRENT') and p.status = 'PAID' then p.amount_paid  else 0 end) as subscription_revenue_paid,
(case when payment_type in ('SHIPMENT') and p.status = 'PAID' then p.amount_paid  else 0 end) as shipment_cost_paid,
(case when payment_type = 'REPAIR COST' and p.status = 'PAID' then p.amount_paid else 0 end) as repair_cost_paid,
(case when payment_type in ('CUSTOMER BOUGHT') and p.status = 'PAID' then p.amount_paid  else 0 end) as customer_bought_paid,
(case when payment_type in ('DEBT COLLECTION') and p.status = 'PAID' then p.amount_paid  else 0 end) as debt_collection_paid,
(case when payment_type in ('ADDITIONAL CHARGE','COMPENSATION') and p.status = 'PAID' then p.amount_paid  else 0 end) as additional_charge_paid,
(case when payment_type like ('%CHARGE BACK%') and p.status = 'PAID' then p.amount_paid  else 0 end) as chargeback_paid,
(case when payment_type like '%REFUND%' and p.status = 'PAID' then p.amount_paid  else 0 end) as refunds_paid,
p.is_double_charge,
CASE WHEN first_card_created_date IS NOT NULL THEN TRUE ELSE FALSE END AS card_user,
CASE WHEN first_timestamp_payment_successful  IS NOT NULL THEN TRUE ELSE FALSE END AS card_finacial_activated
--
from master.customer_historical c
--
left join ods_data_sensitive.customer_pii c2 
 on c.customer_id=c2.customer_id
--
 LEFT JOIN card_customer cc 
 ON cc.customer_id =c.customer_id
 --
inner join master.payment_all_historical p
 on p.customer_id =c.customer_id
 and p.date=c.date 
 ----
 left join master.subscription_historical s
  on s.subscription_id=p.subscription_id
  and s."date"=p.date
--  
left join master.subscription_historical s2 
 on c.customer_acquisition_subscription_id=s2.subscription_id
 and c."date"=s2."date"
--
left join master.order_historical o 
  on o.order_id=p.order_id
  and o."date"=p.date
 --
where c.date=date_trunc('month',current_date)-1
and (p.status not in ('CANCELLED')
 		and not (p.payment_type = 'FIRST' and p.status in ('FAILED','FAILED FULLY'))
 		and p.due_date::date < c."date"-1)
;

