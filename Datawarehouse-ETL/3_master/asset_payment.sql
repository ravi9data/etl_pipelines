BEGIN;

TRUNCATE TABLE master.asset_payment;

INSERT INTO master.asset_payment
SELECT * FROM ods_production.payment_asset;

COMMIT;
