BEGIN;

DELETE FROM master.refund_payment;

INSERT into master.refund_payment 
SELECT * FROM ods_production.payment_refund;

COMMIT;
