
DELETE FROM marketing.partnership_validated_orders
USING master.order ord
WHERE partnership_validated_orders.order_id = ord.order_id
AND ord.created_date::DATE >= DATE_ADD('day', -15, CURRENT_DATE);

INSERT INTO marketing.partnership_validated_orders
SELECT
    src.affiliate_network,
    src.affiliate,
    src.order_id,
    src.marketing_campaign,
    src.new_recurring,
    src.order_status,
    src.affiliate_country,
    src.store_country,
    src.customer_type,
    src.voucher_code,
    src.basket_size,
    src.submitted_date,
    src.currency,
    src.commission_approval,
    src._commission_type AS commission_type,
    src._commission_amount AS commission_amount,
    CURRENT_DATE loaded_at
FROM marketing.v_partnership_order_validation src
         LEFT JOIN marketing.partnership_validated_orders trg ON src.order_id = trg.order_id AND LOWER(src.affiliate_network) = LOWER(trg.affiliate_network)
WHERE trg.order_id IS NULL;
