BEGIN;

--Delete from the target table
DELETE FROM marketing.voucherify_validation_rules_extracted
USING staging.voucherify_validation_rules_extracted
WHERE marketing.voucherify_validation_rules_extracted.val_rule_id = staging.voucherify_validation_rules_extracted.val_rule_id;

--Insert new records
INSERT INTO marketing.voucherify_validation_rules_extracted
SELECT *
FROM staging.voucherify_validation_rules_extracted;

COMMIT;

