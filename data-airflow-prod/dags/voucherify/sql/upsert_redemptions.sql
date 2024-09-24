BEGIN;

--Delete from the target table
DELETE FROM marketing.voucherify_redemption
USING staging.voucherify_redemption
WHERE marketing.voucherify_redemption.id = staging.voucherify_redemption.id;

--Insert new records
INSERT INTO marketing.voucherify_redemption
SELECT *
FROM staging.voucherify_redemption;

COMMIT;
