drop table if exists dwh.customer_segmentation;
create table dwh.customer_segmentation as 
with subs as (
    select
      distinct 
      DATE,
      customer_id,
      MAX(cancellation_date) AS MAX_CANCELLATION_DATE_SUB,
      count(
        Distinct case
          when cancellation_reason_new = 'DEBT COLLECTION'
          and outstanding_assets >= 1 then subscription_id
        end
      ) as dc_subs,
      coalesce(
        count(
          distinct case
            when last_valid_payment_category like '%DEFAULT%'
            and last_valid_payment_category not like ('%RECOVER%')
            and outstanding_assets >= 1 then subscription_id
          end
        ),
        0
      ) as default_subs,
      coalesce(
        count(
          distinct case
            when cancellation_reason_new in (
              'FAILED DELIVERY',
              'LOST BY DHL/SYNERLOGIS',
              'CANCELLED BEFORE SHIPMENT',
              'CANCELLED BEFORE ALLOCATION',
              'REVOCATION'
            )
            or cancellation_reason_churn = 'failed delivery' then subscription_id
          end
        ),
        0
      ) as failed_delivery_subs
    from master.subscription_historical s 
    LEFT JOIN public.dim_dates D ON S.DATE = D.datum
    where (d.day_is_last_of_month=1
    or s."date" = current_date -1)
    group by
      1,
      2
  ),
  orders as (
    select
      DATE,
      customer_id,
      sum(declined_orders) as declined_orders,
      sum(cancelled_orders) as cancelled_orders,
      sum(failed_first_payment_orders) as failed_first_payment_orders,
      max(paid_date) as max_paid_date,
      max(submitted_date) as max_submitted_date,
      count(
        distinct case
          when status = 'APPROVED' then order_id
        end
      ) as approved_offline_orders,
      count(
        distinct case
          when status in (
            'MANUAL REVIEW',
            'PENDING PAYMENT',
            'PENDING APPROVAL',
            'PENDING PROCESSING',
            'ON HOLD'
          ) then order_id
        end
      ) as pending_orders
    from master.order_historical o 
    LEFT JOIN public.dim_dates D ON O.DATE = D.datum
    where (d.day_is_last_of_month=1
    or o."date" = current_date -1)
    group by
      1,
      2
  ),
  b as (
    select
      C."date",
      c.customer_id,
      c.created_at :: date AS CUSTOMER_CREATED_DATE,
      C."date" - c.created_at :: date as days_since_registration,
      case
        when coalesce(active_subscriptions, 0) = 0 then C."date" - c.max_cancellation_date :: date
      end AS days_since_churn,
      c.completed_orders,
      coalesce(c.declined_orders,o.declined_orders) as declined_orders,
      o.failed_first_payment_orders,
      o.cancelled_orders,
      o.pending_orders,
      c.paid_orders,
      max_paid_date,
      max_submitted_date,
      c.clv,
      c.schufa_class,
      c.burgel_risk_category,
      c.delivered_allocations,
      failed_delivery_subs,
      c.returned_allocations,
      c.rfm_segment,
      c.subscriptions,
      c.subscription_durations,
      c.active_subscriptions,
      c.subscription_limit,
      case
        when s.dc_subs >= 1 then 'Debt Collection Customer'
        when s.default_subs >= 1
        then 'Active Delinquent'
        when (
          c.schufa_class in ('N', 'M', 'O', 'P')
          or c.burgel_risk_category in ('4_High')
        )
        and (coalesce(subscriptions, 0) = 0) then 'Blacklisted User'
        when coalesce(c.completed_orders, 0) = 0
        and C."date" - c.created_at :: date <= 30 * 3 then 'Dormant Lead' 
        when coalesce(c.completed_orders, 0) = 0
        then 'Lapsed Lead'
        when subscriptions = failed_delivery_subs
        or  (
          paid_orders = 1
          and coalesce(active_subscriptions, 0) = 0
          and coalesce(delivered_allocations, 0) = 0
        ) then 'Failed Delivery Only' 
        when completed_orders = coalesce(c.declined_orders,o.declined_orders,0) + coalesce(failed_first_payment_orders, 0) + coalesce(cancelled_orders, 0) + coalesce(approved_offline_orders, 0)
        and coalesce(c.declined_orders,o.declined_orders,0) + coalesce(failed_first_payment_orders, 0) + coalesce(cancelled_orders, 0) + coalesce(approved_offline_orders, 0) > 0
        and coalesce(c.subscriptions, 0) = 0 
        then 'Declined or Cancelled Lead'
        when 
	    (
         completed_orders = 1
          and paid_orders = 0
          and C."date" <= max_submitted_Date :: date + 28    
        )
         or
	       (
          paid_orders = 1
          and coalesce(active_subscriptions, 0) >= 1
          and C."date" <= max_submitted_Date :: date + 28
        ) 
        or (
          coalesce(active_subscriptions, 0) = 0
          and pending_orders >= 1
        )
        then 'In Transit'
        when c.active_subscriptions = 1
        and c.subscriptions = 1
        and C."date" >= max_submitted_Date :: date + 28
        and c.subscription_durations in ('Pay As You Go', '1 Months') then 'Paying' || ' ' || 'Cheapies'
        when c.active_subscriptions >= 1 --  and current_date>=max_submitted_Date::date+28
        and c.rfm_segment in (
          'Loyal',
          'Best',
          'Big_Spenders',
          'Unclassified'
        ) then 'Paying' || ' ' || rfm_segment
        when coalesce(c.active_subscriptions, 0) = 0
        and c.subscriptions >= 1
        and c.delivered_allocations >= 1
        and C."date" - COALESCE(
          c.max_cancellation_date,
          MAX_CANCELLATION_DATE_SUB
        ) :: date <= 30 * 6 then 'Warm Churn'
        when coalesce(c.active_subscriptions, 0) = 0
        and c.subscriptions >= 1
        and c.delivered_allocations >= 1
        and C."date" - COALESCE(
          c.max_cancellation_date,
          MAX_CANCELLATION_DATE_SUB
        ) :: date <= 30 * 12 then 'Cold Churn'
        when coalesce(c.active_subscriptions, 0) = 0 
        and c.delivered_allocations >= 1
        and C."date" - COALESCE(
          c.max_cancellation_date,
          MAX_CANCELLATION_DATE_SUB
        ) :: date > 30 * 12 then 'Lost Due to Inactivity'
        else 'Unclassified'
      end as customer_status
    from master.customer_HISTORICAL c
    LEFT JOIN public.dim_dates D ON C.DATE = D.datum
    left join subs s 
      on c.customer_id = s.customer_id
      AND C."date" = S.DATE
    left join orders o 
      on c.customer_id = o.customer_id
      AND C."date" = O.DATE
    where (d.day_is_last_of_month=1
    or C."date" = current_date -1)
    )
    select
B.*,
lead(customer_status) over (partition by customer_id order by date) as next_month_status,
lag(customer_status) over (partition by customer_id order by date) as previous_month_status
from b;

GRANT SELECT ON dwh.customer_segmentation TO tableau;
