drop table if exists ods_production.payment_subscription_details;

create table ods_production.payment_subscription_details as

with a as (
select distinct
pd.subscription_payment_id,
pd.subscription_id,
pd.paid_date,
GREATEST(pd.updated_at, aa.updated_at) as updated_at,
max(due_date) over (partition by pd.subscription_id) as max_due_date,
    COALESCE(min(
        CASE
            WHEN pd.paid_date IS NULL
             THEN pd.due_date
            ELSE NULL::timestamp without time zone
        END) OVER (PARTITION BY pd.subscription_id),
       max(
        CASE
            WHEN pd.status <> 'CANCELLED'
             and pd.paid_date IS NOT NULL THEN pd.due_date
            ELSE NULL::timestamp without time zone
        END) OVER (PARTITION BY pd.subscription_id)) AS next_due_date,
            COALESCE(min(
        CASE
            WHEN pd.paid_date IS NULL
             and pd.asset_return_date is null
             and aa.is_last_allocation_per_asset =true
               THEN pd.due_date
            ELSE NULL::timestamp without time zone
        END) OVER (PARTITION BY pd.asset_id),
       max(
        CASE
            WHEN pd.status <> 'CANCELLED'
             and pd.paid_date IS NOT NULL
          THEN pd.due_date
            ELSE NULL::timestamp without time zone
        END) OVER (PARTITION BY pd.asset_id)) AS next_due_date_asset,
            COALESCE(min(
        CASE
            WHEN pd.paid_date IS NULL
             and pd.asset_return_date is null
             and aa.is_last_allocation_per_asset =true
               THEN pd.due_date
            ELSE NULL::timestamp without time zone
        END) OVER (PARTITION BY pd.allocation_id),
       max(
        CASE
            WHEN pd.status <> 'CANCELLED'
             and pd.paid_date IS NOT NULL
          THEN pd.due_date
            ELSE NULL::timestamp without time zone
        END) OVER (PARTITION BY pd.allocation_id)) AS next_due_date_allocation,
   COALESCE(pd.paid_date::date, 'now'::text::date) - pd.due_date::date AS dpd,
         CASE
            WHEN pd.amount_due::DECIMAL(17,2) = 0::numeric THEN 'PAID_TIMELY'::text
            WHEN pd.paid_date IS NOT NULL AND
             COALESCE(pd.refund_amount, 0::numeric)::numeric(38,2) = COALESCE(pd.amount_paid, 0::numeric)
              AND abs(pd.amount_voucher) <= pd.amount_subscription THEN 'PAID_REFUNDED'::text
             WHEN pd.paid_date IS NOT NULL AND
             COALESCE(pd.chargeback_amount, 0::numeric)::numeric(38,2) = COALESCE(pd.amount_paid, 0::numeric)
              AND abs(pd.amount_voucher) <= pd.amount_subscription THEN 'PAID_CHARGEBACK'::text
            WHEN pd.paid_date IS NOT NULL AND (COALESCE(pd.paid_date::date, 'now'::text::date) - pd.due_date::date) <= 1
             and (/*is_paid_partially not in ('partial payment - outstanding') or*/ amount_due::DECIMAL(17,2)=amount_paid::DECIMAL(17,2))
  			THEN 'PAID_TIMELY'::text
            WHEN pd.paid_date IS NOT NULL AND (COALESCE(pd.paid_date::date, 'now'::text::date) - pd.due_date::date) <= 5
             and (/*is_paid_partially not in ('partial payment - outstanding') or*/ amount_due::DECIMAL(17,2)=amount_paid::DECIMAL(17,2))
  			THEN 'RECOVERY_ARREARS'::text
            WHEN pd.paid_date IS NOT NULL AND (COALESCE(pd.paid_date::date, 'now'::text::date) - pd.due_date::date) <= 15
             and (/*is_paid_partially not in ('partial payment - outstanding') or*/ amount_due::DECIMAL(17,2)=amount_paid::DECIMAL(17,2))
  			THEN 'RECOVERY_DELINQUENT_EARLY'::text
            WHEN pd.paid_date IS NOT NULL AND (COALESCE(pd.paid_date::date, 'now'::text::date) - pd.due_date::date) <= 30
             and (/*is_paid_partially not in ('partial payment - outstanding') or */amount_due::DECIMAL(17,2)=amount_paid::DECIMAL(17,2))
  			THEN 'RECOVERY_DELINQUENT_LATE'::text
            WHEN pd.paid_date IS NOT NULL AND (COALESCE(pd.paid_date::date, 'now'::text::date) - pd.due_date::date) > 30
             and (/*is_paid_partially not in ('partial payment - outstanding') or */amount_due::DECIMAL(17,2)=amount_paid::DECIMAL(17,2))
             /*and is_paid_partially not in ('partial payment - outstanding')*/
  			THEN 'RECOVERY_DEFAULT'::text
            WHEN (pd.paid_date IS NULL) AND (COALESCE(pd.paid_date::date, 'now'::text::date) - pd.due_date::date) <= 0 THEN 'NYD'::text
            WHEN ((pd.paid_date IS NULL /*or is_paid_partially in ('partial payment - outstanding')*/) AND (COALESCE(pd.paid_date::date, 'now'::text::date) - pd.due_date::date) <= 5)
             or (pd.paid_date is not null and (coalesce(pd.chargeback_amount,0)>=coalesce(amount_paid,0) or amount_paid::DECIMAL(17,2)<amount_due::DECIMAL(17,2)) AND 'now'::text::date - coalesce(pd.min_chargeback_date::date,paid_date::date,due_date::date) <= 5) THEN 'ARREARS'::text
            WHEN ((pd.paid_date IS NULL /*or is_paid_partially in ('partial payment - outstanding')*/) AND (COALESCE(pd.paid_date::date, 'now'::text::date) - pd.due_date::date) <= 30)
             or (pd.paid_date is not null and (coalesce(pd.chargeback_amount,0)>=coalesce(amount_paid,0) or amount_paid::DECIMAL(17,2)<amount_due::DECIMAL(17,2)) AND 'now'::text::date - coalesce(pd.min_chargeback_date::date,paid_date::date,due_date::date) <= 30) THEN 'DELINQUENT'::text
            WHEN ((pd.paid_date IS NULL /*or is_paid_partially in ('partial payment - outstanding')*/) AND (COALESCE(pd.paid_date::date, 'now'::text::date) - pd.due_date::date) <= 60)
             or (pd.paid_date is not null and (coalesce(pd.chargeback_amount,0)>=coalesce(amount_paid,0) or amount_paid::DECIMAL(17,2)<amount_due::DECIMAL(17,2)) AND 'now'::text::date - coalesce(pd.min_chargeback_date::date,paid_date::date,due_date::date) <= 60) THEN 'DEFAULT_60'::text
			WHEN ((pd.paid_date IS NULL /*or is_paid_partially in ('partial payment - outstanding')*/) AND (COALESCE(pd.paid_date::date, 'now'::text::date) - pd.due_date::date) <= 90)
             or (pd.paid_date is not null and (coalesce(pd.chargeback_amount,0)>=coalesce(amount_paid,0) or amount_paid::DECIMAL(17,2)<amount_due::DECIMAL(17,2)) AND 'now'::text::date - coalesce(pd.min_chargeback_date::date,paid_date::date,due_date::date) <= 90) THEN 'DEFAULT_90'::text
            WHEN ((pd.paid_date IS NULL /*or is_paid_partially in ('partial payment - outstanding')*/) AND (COALESCE(pd.paid_date::date, 'now'::text::date) - pd.due_date::date) <= 180)
             or (pd.paid_date is not null and (coalesce(pd.chargeback_amount,0)>=coalesce(amount_paid,0) or amount_paid::DECIMAL(17,2)<amount_due::DECIMAL(17,2)) AND 'now'::text::date - coalesce(pd.min_chargeback_date::date,paid_date::date,due_date::date) <= 180) THEN 'DEFAULT_180'::text
            WHEN (pd.paid_date IS NULL /*or is_paid_partially in ('partial payment - outstanding')*/)
             or (pd.paid_date is not null and (coalesce(pd.chargeback_amount,0)>=coalesce(amount_paid,0) or amount_paid::DECIMAL(17,2)<amount_due::DECIMAL(17,2)))
              THEN 'DEFAULT_360'::text
            ELSE NULL::text
        END AS subscription_payment_category,
      CASE
            WHEN pd.first_asset_delivery_date IS NOT NULL AND pd.payment_number = 1 AND (asset_return_date - pd.due_date::date) <= 14 THEN 'eligible for refund'::text
            WHEN pd.first_asset_delivery_date IS NOT NULL AND pd.payment_number > 1 AND (asset_return_date::date - pd.due_date::date) <= 7 THEN 'eligible for refund'::text
            WHEN pd.first_asset_delivery_date IS NULL AND pd.subsription_cancellation_date IS NOT NULL THEN 'eligible for refund'::text
            WHEN pd.amount_paid::DECIMAL(17,2) > pd.amount_due::DECIMAL(17,2) THEN 'eligible for refund'::text
            ELSE NULL::text
        END AS is_eligible_for_refund,
    case when pd.status ='PLANNED' and pd.due_Date < current_date then true else false end as is_not_triggered_payments
from ods_production.payment_subscription pd
left join ods_production.allocation aa on pd.allocation_id=aa.allocation_id
where pd.status <> 'CANCELLED')
select distinct
*
from a;


GRANT SELECT ON ods_production.payment_subscription_details TO group finance;
grant select on ods_production.payment_subscription_details to basri_oz;
GRANT SELECT ON ods_production.payment_subscription_details TO tableau;
