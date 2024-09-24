WITH dc AS
    (SELECT customer_id,
            sum(dc_amount_due -
                recovered_value -
                net_dc_amount_paid) AS outstanding_dc_amount,
            sum(outstanding_assets) AS outstanding_assets,
            sum(dc_amount_due) AS dc_debt,
            sum(coalesce(net_dc_amount_paid,0)) AS dc_paid,
            sum(CASE
                    WHEN cancelled_dc_payments = dc_payments THEN 1
                    ELSE 0
                END) AS n_cancelled_from_dc
     FROM ods_production.debt_collection
     GROUP BY 1),
     def AS
    (SELECT dc.customer_id,
            sum(amount_due) AS defaulted_amount
     FROM ods_production.payment_subscription spc
     JOIN ods_production.debt_collection dc ON dc.subscription_id = spc.subscription_id
     WHERE due_date::date < dateadd('month', -3, paid_date::date)
     GROUP BY 1),
     ndc AS
    (SELECT s.customer_id,
            count(*) subscriptions,
            sum(CASE
                    WHEN s.cancellation_reason_new = 'DEBT COLLECTION' THEN 1
                    ELSE 0
                END) AS n_dc_subs,
            CASE
                WHEN n_dc_subs > 0 THEN TRUE
                ELSE FALSE
            END AS is_dc,
            sum(s.delivered_assets) AS delivered_allocations,
            sum(s.outstanding_asset_value) AS outstanding_dc_amount_v2,
            sum(s.subscription_value) AS total_subscription_value,
            sum(s.committed_sub_value) AS total_committed_value,
            max(CASE
                    WHEN s.dpd < 0
                         OR s.outstanding_subscription_revenue = 0 THEN 0
                    ELSE s.dpd
                END) AS max_dpd_master
     FROM master.subscription s
     WHERE s.first_subscription_start_date <= '{sub_start_day}'
     GROUP BY 1),
     sp AS
    (SELECT sp.customer_id,
            count(DISTINCT ps.payment_method) n_payment_methods,
            avg(COALESCE(sp.paid_date::date, 'now'::text::date) -
                         sp.due_date::date) AS avg_dpd,
            avg(CASE
                    WHEN sp.paid_date >= '2019-01-01' THEN
                        (COALESCE(sp.paid_date::date, 'now'::text::date) -
                         sp.due_date::date)
                    ELSE 0
                END) AS avg_dpd_recent_1,
            avg(CASE
                    WHEN sp.paid_date >= dateadd('year', -2,'now'::text::date) THEN
                        (COALESCE(sp.paid_date::date, 'now'::text::date) -
                         sp.due_date::date)
                    ELSE 0
                END) AS avg_dpd_recent,
            sum(CASE
                    WHEN sp.paid_date::date > s.debt_collection_handover_date::date THEN 1
                    ELSE 0
                END) AS n_payments_DA,
            CASE
                WHEN count(sp.paid_date) = count(DISTINCT sp.subscription_id) THEN 1
                ELSE 0
            END AS paid_only_first_payments,
            min(CASE
                    WHEN sp.due_date::date < dateadd('month',
                                                     -3,
                                                     coalesce(sp.paid_date::date,
                                                              'now'::text::date)) THEN
                        sp.due_date
                    ELSE NULL
                END) first_failed,
            min(CASE
                    WHEN sp.due_date::date < dateadd('month',
                                                     -1,
                                                     coalesce(sp.paid_date::date,
                                                              'now'::text::date)) THEN
                        sp.due_date
                    ELSE NULL
                END) first_failed_2,
            dateadd('month', -1, first_failed) AS last_paid,
            months_between(first_failed::date,
                           min(s.start_date::date)) AS time_before_failed,
            months_between(first_failed_2::date,
                           min(s.start_date::date)) AS time_before_failed_
     FROM master.subscription_payment sp
     LEFT JOIN ods_production.subscription s ON sp.subscription_id = s.subscription_id
     LEFT JOIN ods_production.payment_subscription ps ON
         ps.subscription_payment_id = sp.payment_id
     WHERE sp.amount_due != 0
         AND sp.status != 'PLANNED'
         AND sp.status != 'HELD'
         AND sp.subscription_id IS NOT NULL
     GROUP BY 1),
     ord AS
    (SELECT o.spree_customer_id__c AS customer_id,
            sum(CASE
                    WHEN o.status = 'DECLINED'
                         AND o.createddate >= sp.last_paid THEN 1
                    ELSE 0
                END) n_orders_declined_last_month,
            sum(CASE
                    WHEN o.createddate >= sp.last_paid THEN o.totalamount
                    ELSE 0
                END) total_value_last_month,
            sum(CASE
                    WHEN o.status = 'CANCELLED'
                         AND o.state_approved__c IS NOT NULL THEN o.totalamount
                    ELSE 0
                END) tot_value_cancelled_orders,
            sum(CASE
                    WHEN o.status != 'CANCELLED' THEN o.totalamount
                    ELSE 0
                END) tot_value_not_cancelled_orders
     FROM stg_salesforce."order" o
     LEFT JOIN sp ON sp.customer_id = o.spree_customer_id__c
     WHERE o.createddate <= sp.first_failed
     GROUP BY 1),
     del AS
    (SELECT spc.subscription__c,
            dc.customer_id,
            max(date_paid__c) last_paid,
            coalesce(max(dc.debt_collection_handover_date),
                     max(dc.cancellation_date)) last_canc,
            months_between(last_paid::date, last_canc::date) AS delay_for_dc
     FROM stg_salesforce.subscription_payment__c spc
     JOIN ods_production.debt_collection dc ON dc.subscription_id = spc.subscription__c
     GROUP BY 1,
              2),
     d AS
    (SELECT customer_id,
            max(delay_for_dc) max_delay_for_dc
     FROM del
     GROUP BY 1),
     ap AS
    (SELECT customer_id,
            sum(coalesce(amount_due,0)) AS asset_payment_due,
            sum(CASE
                    WHEN ap.status = 'PAID'
                         AND ap.payment_type = 'CUSTOMER BOUGHT'
                         AND coalesce(ap.amount_paid,0) = 0 THEN coalesce(amount_due,0)
                    ELSE coalesce(amount_paid,0)
                END) AS asset_paid,
            asset_payment_due-asset_paid AS outstanding_asset_payment
     FROM master.asset_payment ap
     WHERE ((ap.status = 'NOT PAID'
             AND ap.sold_date IS NOT NULL
             AND ap.payment_type = 'CUSTOMER BOUGHT'))
         OR ((ap.status != 'NOT PAID'
              AND ap.payment_type IN ('REPAIR COST',
                                      'CUSTOMER BOUGHT',
                                      'ADDITIONAL CHARGE')))
         OR ((ap.status = 'NOT PAID'
              AND ap.payment_type IN ('REPAIR COST',
                                      'ADDITIONAL CHARGE')))
     GROUP BY 1),
     dm AS
    (SELECT customer_id,
            sum(CASE
                    WHEN status IN ('PLANNED','HELD')
                         AND due_date::date < 'now'::text::date THEN coalesce(amount_due,0)
                    ELSE 0
                END) AS value_past_held_planned
     FROM master.subscription_payment sp
     GROUP BY 1)
SELECT c.customer_id,
       c.active_subscription_value,
       c.subscription_revenue_due,
       c.subscription_revenue_paid,
       c.subscription_revenue_chargeback,
       c.subscription_revenue_refunded AS subscription_revenue_refund,
       c.payment_count,
       c.paid_subscriptions,
       c.refunded_subscriptions,
       c.failed_subscriptions,
       c.chargeback_subscriptions,
       c.failed_subscriptions::float/c.payment_count AS failed_percent,
       CASE
           WHEN c.trust_type = 'blacklisted' THEN TRUE
           ELSE FALSE
       END AS is_blacklisted,
       dc.outstanding_dc_amount,
       dc.outstanding_assets,
       dc.dc_debt,
       dc.n_cancelled_from_dc,
       dc.dc_paid,
       ndc.subscriptions,
       ndc.n_dc_subs,
       ndc.is_dc,
       ndc.total_subscription_value,
       ndc.total_committed_value,
       ndc.max_dpd_master,
       ndc.outstanding_dc_amount_v2,
       sp.avg_dpd,
       sp.avg_dpd_recent,
       sp.avg_dpd_recent_1,
       sp.n_payment_methods,
       sp.n_payments_DA,
       sp.paid_only_first_payments,
       sp.time_before_failed,
       sp.time_before_failed_,
       ord.n_orders_declined_last_month,
       ord.total_value_last_month,
       ord.tot_value_cancelled_orders,
       ord.tot_value_not_cancelled_orders,
       def.defaulted_amount,
       d.max_delay_for_dc,
       ap.asset_payment_due,
       ap.asset_paid,
       ap.outstanding_asset_payment,
       dm.value_past_held_planned,
       c.subscription_revenue_due -
           dm.value_past_held_planned -
           c.subscription_revenue_paid AS outstanding_amount
FROM master.customer c
--customers with first subscription started before 3 months ago: join and not left-join
LEFT JOIN dc ON c.customer_id = dc.customer_id
JOIN ndc ON c.customer_id = ndc.customer_id
LEFT JOIN sp ON sp.customer_id = c.customer_id
LEFT JOIN ord ON ord.customer_id = c.customer_id
LEFT JOIN def ON def.customer_id = c.customer_id
LEFT JOIN d ON d.customer_id = c.customer_id
LEFT JOIN ap ON ap.customer_id = c.customer_id
LEFT JOIN dm ON dm.customer_id = c.customer_id
WHERE ndc.delivered_allocations > 0
    AND customer_type = 'normal_customer'
    AND ((c.subscription_revenue_paid > 0)
         OR (c.subscription_revenue_paid = 0
             AND c.failed_subscriptions > 2));
