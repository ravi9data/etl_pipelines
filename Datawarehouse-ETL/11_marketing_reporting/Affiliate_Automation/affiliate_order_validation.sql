
DELETE FROM marketing.affiliate_validated_orders
USING master.order ord
WHERE affiliate_validated_orders.order_id = ord.order_id
AND COALESCE(ord.paid_date::DATE,ord.submitted_date::DATE,ord.created_date::DATE) >= DATE_ADD('day', -15, CURRENT_DATE);

INSERT INTO marketing.affiliate_validated_orders

SELECT
    src.affiliate_network,
    src.affiliate,
    src.order_id,
    src.new_recurring,
    src.order_status,
    src.affiliate_country,
    src.order_country,
    src.click_date,
    src.created_date,
    src.submitted_date,
    src.currency,
    src.exchange_rate,
    CASE WHEN src.commission_approval IN ('APPROVED', 'PENDING') AND ep.publisher IS NOT NULL THEN 'DECLINED'
         WHEN src.commission_approval IN ('APPROVED', 'PENDING') AND src.affiliate ILIKE '%Interactive Performance GmbH%' THEN src.commission_approval
         WHEN src.commission_approval IN ('APPROVED', 'PENDING') AND src.affiliate ILIKE '%Shoparize NL%' THEN src.commission_approval
         WHEN src.commission_approval IN ('APPROVED', 'PENDING') AND src.affiliate = 'ES Vip District' AND o.voucher_code ILIKE 'VIPD-%' THEN src.commission_approval
         WHEN src.commission_approval IN ('APPROVED', 'PENDING') AND src.affiliate = 'vfhappybox' AND o.voucher_code ILIKE 'VF-%' THEN src.commission_approval
         WHEN src.commission_approval IN ('APPROVED', 'PENDING') AND o.marketing_channel != 'Affiliates' THEN 'DECLINED'
         ELSE src.commission_approval END AS commission_approval,
    src._commission_type AS commission_type,
    src._commission_amount AS commission_amount,
    CURRENT_DATE AS loaded_at,
    NULL AS affiliate_network_sync_status,
    o.customer_id,
    src.click_id,
    src.paid_date
FROM marketing.v_affiliate_order_validation src
         LEFT JOIN marketing.affiliate_validated_orders trg ON src.order_id = trg.order_id AND LOWER(src.affiliate_network) = LOWER(trg.affiliate_network)
         LEFT JOIN staging_airbyte_bi.affiliate_excluded_publishers ep ON LOWER(src.affiliate) = LOWER(ep.publisher) AND src.created_date::DATE >= '2023-01-01'
         LEFT JOIN master.order o ON o.order_id = src.order_id
WHERE trg.order_id IS NULL;
