INSERT INTO stg_salesforce.subscription_payment_history
SELECT id, isdeleted , parentid , createdbyid , createddate,
field,oldvalue, newvalue FROM stg_skyvia.test_subscription_payment_history WHERE id NOT IN (
SELECT id  FROM stg_salesforce.subscription_payment_history);

-- -------------------------------------------------------