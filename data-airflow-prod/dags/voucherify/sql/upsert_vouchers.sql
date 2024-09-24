BEGIN;

-- Delete from the target table
DELETE FROM marketing.voucherify_voucher
USING staging.voucherify_voucher
WHERE marketing.voucherify_voucher.id = staging.voucherify_voucher.id;

-- Temp table to remove duplicates
CREATE TEMP TABLE temp_deduped_sg_voucherify_voucher AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY code, id, campaign_id, validation_rules_id 
               ORDER BY 
                   CASE 
                       WHEN last_update_date IS NOT NULL THEN 0 
                       ELSE 1 
                   END, 
                   last_update_date DESC
           ) as rn
    FROM staging.voucherify_voucher
) sub
WHERE rn = 1;

-- Insert new deduplicated records
INSERT INTO marketing.voucherify_voucher (
    code, voucher_type, "value", discount_type, campaign, category, "start_date", expiration_date, gift_balance, 
    loyalty_card_balance, redemption_limit, redemption_count, active, qr_code, barcode, id, is_referral_code, 
    creation_date, last_update_date, discount_amount_limit, campaign_id, additional_info, customer_id, 
    discount_unit_type, discount_unit_effect, customer_source_id, validation_rules_id, allocation, asset, locale, 
    "order", payment, recurring, request
)
SELECT 
    code, voucher_type, "value", discount_type, campaign, category, "start_date", expiration_date, gift_balance, 
    loyalty_card_balance, redemption_limit, redemption_count, active, qr_code, barcode, id, is_referral_code, 
    creation_date, last_update_date, discount_amount_limit, campaign_id, additional_info, customer_id, 
    discount_unit_type, discount_unit_effect, customer_source_id, validation_rules_id, allocation, asset, locale, 
    "order", payment, recurring, request
FROM temp_deduped_sg_voucherify_voucher;

-- Drop the temporary table
DROP TABLE temp_deduped_sg_voucherify_voucher;

COMMIT;
