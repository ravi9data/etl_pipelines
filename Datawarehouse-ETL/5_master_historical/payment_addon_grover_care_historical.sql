BEGIN;

where date = current_date - 1;

SELECT 
    payment_id,
    resource_id,
    movement_id,
    payment_group_id,
    psp_reference_id,
    customer_id,
    order_id,
    subscription_id,
    payment_type,
    payment_number,
    created_at,
    updated_at,
    status,
    due_date,
    paid_date,
    money_received_at,
    currency,
    payment_method,
    payment_context_reason,
    invoice_url,
    invoice_number,
    invoice_date,
    amount_due,
    amount_paid,
    amount_tax,
    tax_rate,
    pending_date,
    failed_date,
    attempts_to_pay,
    failed_reason,
    refund_date,
    refund_amount,
    country_name,
    current_date - 1
FROM 
    WHERE created_at < current_date;

COMMIT;