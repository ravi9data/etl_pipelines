BEGIN;

--Delete from the target table
DELETE FROM marketing.voucherify_validation_rules
USING staging.voucherify_validation_rules
WHERE marketing.voucherify_validation_rules.id = staging.voucherify_validation_rules.id;

--Insert new records
INSERT INTO marketing.voucherify_validation_rules
SELECT *
FROM staging.voucherify_validation_rules;

COMMIT;

