SELECT op.customer_id, 
       op.order_id,
       op.payment_method, 
       op.payment_type,
       json_extract_path_text(op.payment_metadata, 'paypal_verified') as paypal_verified,
       json_extract_path_text(op.payment_metadata, 'credit_card_bank_name') as credit_card_bank_name,
       ncb.card_issuer
FROM stg_order_approval.order_payment op
LEFT JOIN stg_order_approval.neutrino_card_bin ncb 
    ON ncb.card_bin_number = json_extract_path_text(op.payment_metadata, 'credit_card_bin');
