-------------Card_subscription_payment
SELECT
DISTINCT 
ps.subscription_payment_id,
s.subscription_id ,
s.customer_id,
ps.payment_type,
ps.payment_number::integer,
CASE WHEN due_date > first_card_created_date THEN
    ROW_NUMBER ()OVER (PARTITION BY s.subscription_id ORDER BY (CASE WHEN due_date > first_card_created_date THEN due_date ELSE NULL  END)ASC )
    ELSE NULL END as card_payment_number,
ps.created_at,
s.status AS subscription_status,
ps.status,
s.start_date,
ps.due_date,
ps.paid_date,
s.first_card_created_date ,
ps.failed_date,
ps.paid_status,
ps.currency,
ps.payment_method,
ps.amount_subscription,
s.subscription_value,
ps.amount_due,
ps.amount_due_without_taxes,
ps.amount_paid,
ps.amount_discount,
ps.amount_voucher,
ps.amount_shipment,
ps.amount_overdue_fee,
ps.refund_amount,
ps.chargeback_amount,
ps.capital_source
FROM ods_production.payment_subscription ps 
ON ps.subscription_id=s.subscription_id;
