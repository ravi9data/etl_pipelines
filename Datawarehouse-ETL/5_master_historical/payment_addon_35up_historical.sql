BEGIN;

DELETE FROM master.payment_addon_35up_historical
WHERE date = current_date - 1;

INSERT INTO master.payment_addon_35up_historical
SELECT 
    payment_id, 
    resource_id, 
    movement_id, 
    psp_reference_id, 
    customer_id, 
    order_id, 
    addon_id, 
    addon_name, 
    payment_type, 
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
    current_date -1 AS date
FROM 
    master.payment_addon_35up
WHERE created_at < current_date;

COMMIT;