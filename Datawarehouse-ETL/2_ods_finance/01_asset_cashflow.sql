drop table if exists ods_production.asset_cashflow;
CREATe table  ods_production.asset_cashflow as 
WITH a AS (
         SELECT 
			aa.asset_id,
		    max(sp.paid_date) AS max_paid_date,
            min(sp2.next_due_date_asset) as default_date,
            count(DISTINCT
                CASE
                    WHEN (sp.due_date < 'now'::text::date or sp.paid_date is not null) 
                    AND sp2.subscription_payment_category <> 'PAID_REFUNDED'
                    AND sp2.subscription_payment_category <> 'PAID_CHARGEBACK'
                    tHEN sp.subscription_payment_id
                    ELSE NULL::character varying
                END) AS payment_count,
            count(DISTINCT
                CASE
                    WHEN sp.paid_date IS NOT NULL 
                     AND sp2.subscription_payment_category <> 'PAID_REFUNDED'::text 
                     AND sp2.subscription_payment_category <> 'PAID_CHARGEBACK'::text 
                      THEN sp.subscription_payment_id
                    ELSE NULL::character varying
                END) AS paid_subscriptions,
            count(DISTINCT
                CASE
                    WHEN sp.paid_date IS NOT NULL 
                     AND sp2.subscription_payment_category = 'PAID_REFUNDED'::text 
                     AND sp2.subscription_payment_category <> 'PAID_CHARGEBACK'::text 
                      THEN sp.subscription_payment_id
                    ELSE NULL::character varying
                END) AS refunded_subscriptions,
            max(
                CASE
                    WHEN sp.due_date = Sp2.next_due_date_asset 
                     THEN sp2.subscription_payment_category
                    ELSE NULL
                END) AS last_valid_payment_category,
            max(
                CASE
                    WHEN sp.due_date = Sp2.next_due_date_asset 
                     THEN sp.amount_due
                    ELSE NULL
                END) AS last_payment_amount_due,
            max(
                CASE
                    WHEN sp.due_date = Sp2.next_due_date_asset 
                     THEN sp.amount_paid
                    ELSE NULL
                END) AS last_payment_amount_paid,
            max(
                CASE
                    WHEN sp.due_date = Sp2.next_due_date_asset 
                     THEN sp.status
                    ELSE NULL
                END) AS last_valid_payment_status,
            sum(
                CASE
                    WHEN sp.due_date::date <= 'now'::text::date 
                     THEN COALESCE(sp.amount_due, 0::numeric)
                    ELSE NULL::numeric
                END) AS amount_due,
            COALESCE(sum(
                CASE
                    WHEN sp.paid_date IS NOT NULL 
                     --AND sp2.subscription_payment_category <> 'PAID_REFUNDED'::text 
                     --AND sp2.subscription_payment_category <> 'PAID_CHARGEBACK'::text 
                     THEN coalesce(sp.amount_paid,0)-COALESCE(sp.chargeback_amount, 0::numeric)
                    ELSE NULL::numeric
                END), 0::numeric) AS subscription_revenue,
            COALESCE(sum(
                CASE
                    WHEN sp.paid_date IS NOT NULL 
                     AND sp2.subscription_payment_category <> 'PAID_REFUNDED'::text 
                     AND sp2.subscription_payment_category <> 'PAID_CHARGEBACK'::text 
                     AND sp.paid_date >= ('now'::text::date - 31) 
                      THEN coalesce(sp.amount_paid,0)-COALESCE(sp.chargeback_amount, 0::numeric)
                    ELSE NULL::numeric
                END), 0::numeric) AS subscription_revenue_last_31day,
            COALESCE(sum( 
                CASE
                    WHEN sp.paid_date IS NOT NULL 
                     AND sp2.subscription_payment_category <> 'PAID_REFUNDED'::text
                     AND sp2.subscription_payment_category <> 'PAID_CHARGEBACK'::text  
                     AND date_trunc('month',sp.paid_date)::date =date_trunc('month',CURRENT_DATE)::date
                      THEN coalesce(sp.amount_paid,0)-COALESCE(sp.chargeback_amount, 0::numeric)
                    ELSE NULL::numeric
                END), 0::numeric) AS subscription_revenue_current_month,
            COALESCE(sum( 
                CASE
                    WHEN sp.paid_date IS NOT NULL 
                     AND sp2.subscription_payment_category <> 'PAID_REFUNDED'::text 
                     AND sp2.subscription_payment_category <> 'PAID_CHARGEBACK'::text 
                     AND date_trunc('month',sp.paid_date)::date =(date_trunc('month',CURRENT_DATE) - interval '1 month')::date
                      THEN coalesce(sp.amount_paid,0)-COALESCE(sp.chargeback_amount, 0::numeric)
                    ELSE NULL::numeric
                END), 0::numeric) AS subscription_revenue_last_month,
            sum(
                CASE
                    WHEN sp.due_date < 'now'::text::date 
                     THEN COALESCE(sp.refund_amount, 0::numeric)
                    ELSE NULL::numeric
                END) AS amount_refund,
            sum(
                CASE
                    WHEN sp.due_date < 'now'::text::date 
                     THEN COALESCE(sp.chargeback_amount, 0::numeric)
                    ELSE NULL::numeric
                END) AS amount_chargeback,       
 		    round(avg(
                CASE
                    WHEN sp.amount_subscription = 0 THEN NULL
                    ELSE sp.amount_subscription
                END)) AS avg_subscription_amount,
            round(max(
        	CASE
            	WHEN sp.amount_subscription = 0 THEN NULL
            	ELSE sp.amount_subscription
        	END)) AS max_subscription_amount,
    	    count(DISTINCT sp.subscription_id) AS count_subscriptions
    FROM ods_production.allocation aa
    left join ods_production.payment_subscription sp 
    on sp.allocation_id=aa.allocation_id
    LEFT JOIN ods_production.payment_subscription_details sp2 
    on sp2.subscription_payment_id=sp.subscription_payment_id
    and coalesce(sp2.paid_date,'1990-05-22')=coalesce(sp.paid_date,'1990-05-22')
    WHERE true and sp.status not in ('CANCELLED')
    GROUP BY aa.asset_id
          ), 
        prep AS (
         SELECT 
         	a.asset_id,
            sum(p.amount_paid) AS last_payment_amount
           FROM ods_production.allocation a           
           LEFT JOIN ods_production.payment_subscription p 
              ON a.allocation_id::text = p.allocation_id::text
             LEFT JOIN a a_2 
              ON a.asset_id::text = a_2.asset_id::text
          WHERE p.paid_date = a_2.max_paid_date
          GROUP BY a.asset_id
        ), b AS (   
        SELECT DISTINCT
         	a.asset_id,
            sum(
                CASE
                    WHEN a.payment_type = 'REPAIR COST' 
                     AND a.paid_date IS NOT NULL 
                      THEN COALESCE(a.amount_paid, 0) - COALESCE(a.refund_amount, 0) - COALESCE(a.chargeback_amount, 0)
                    ELSE NULL
                END) AS repair_cost_paid,
                            sum(
                CASE
                    WHEN a.payment_type = 'SHIPMENT' 
                     AND a.paid_date IS NOT NULL 
                      THEN COALESCE(a.amount_paid, 0) - COALESCE(a.refund_amount, 0) - COALESCE(a.chargeback_amount, 0)
                    ELSE NULL
                END) AS shipment_cost_paid,
            sum(
                CASE
                    WHEN a.payment_type = 'CUSTOMER BOUGHT' 
                     AND a.paid_date IS NOT NULL 
                      THEN COALESCE(a.amount_paid, 0) - 
                            COALESCE(a.refund_amount, 0) - 
                             COALESCE(a.chargeback_amount, 0)
                    ELSE NULL::numeric
                END) AS customer_bought_paid,
            sum(
                CASE
                    WHEN a.payment_type = 'GROVER SOLD' AND a.paid_date IS NOT NULL THEN COALESCE(a.amount_paid, 0) - COALESCE(a.refund_amount, 0) - COALESCE(a.chargeback_amount, 0)
                    ELSE NULL::numeric
            sum(
                CASE
                    WHEN a.payment_type in ('ADDITIONAL CHARGE','COMPENSATION') AND a.paid_date IS NOT NULL THEN COALESCE(a.amount_paid, 0) - COALESCE(a.refund_amount, 0) - COALESCE(a.chargeback_amount, 0)
                    ELSE NULL::numeric
                END) AS additional_charge_paid,
               listagg(
                CASE
                    WHEN a.payment_type in ('GROVER SOLD','CUSTOMER BOUGHT') 
                      AND a.paid_date IS NOT NULL 
                       THEN invoice_url
                    ELSE NULL
                END, ' / ') AS asset_sold_invoice,
          		 max(
             	  CASE
                    WHEN a.payment_type in ('GROVER SOLD','CUSTOMER BOUGHT') 
                      AND a.sold_date IS NOT NULL 
                       THEN sold_date END) AS max_asset_sold_date
           FROM ods_production.payment_asset a
           where true and status not in ('CANCELLED')
          GROUP BY a.asset_id 
        )        
 SELECT 
    COALESCE(a.asset_id, b.asset_id) AS asset_id,
    COALESCE(a.amount_due, 0 ) AS subscription_revenue_due,
    COALESCE(a.subscription_revenue, 0) AS subscription_revenue_paid,  --excl. CHB/REF
    COALESCE(a.subscription_revenue_last_month, 0 ) AS subscription_revenue_paid_last_month,--excl. CHB/REF
    COALESCE(a.subscription_revenue_current_month, 0 ) AS subscription_revenue_current_month,
    COALESCE(a.subscription_revenue_last_31day, 0 ) AS subscription_revenue_last_31day,
    COALESCE(a.amount_refund,0) as amount_refund,
    COALESCE(a.payment_count, 0 ) AS sub_payments_due,
    COALESCE(a.paid_subscriptions, 0 ) AS sub_payments_paid,
    coalesce(a.refunded_subscriptions,0) as sub_payments_refunded,
    COALESCE(b.repair_cost_paid, 0 ) AS repair_cost_paid,
    COALESCE(b.customer_bought_paid, 0 ) AS customer_bought_paid,
    asset_sold_invoice,
    max_asset_sold_date,
    COALESCE(b.additional_charge_paid, 0 ) AS additional_charge_paid,
    coalesce(b.shipment_cost_paid,0) as shipment_cost_paid,
    a.max_paid_date,
    p.last_payment_amount,
    a.avg_subscription_amount,
    a.max_subscription_amount,
    a.count_subscriptions,
    last_payment_amount_due,
    last_payment_amount_paid,
    COALESCE(a.subscription_revenue, 0 ) + 
    COALESCE(b.repair_cost_paid, 0 ) + 
    COALESCE(b.customer_bought_paid, 0 ) + 
    COALESCE(b.shipment_cost_paid, 0 ) + 
    COALESCE(b.additional_charge_paid, 0 ) AS total_paid
   FROM a
     FULL JOIN b ON a.asset_id = b.asset_id
     LEFT JOIN prep p ON p.asset_id= a.asset_id;

GRANT SELECT ON ods_production.asset_cashflow TO tableau;
