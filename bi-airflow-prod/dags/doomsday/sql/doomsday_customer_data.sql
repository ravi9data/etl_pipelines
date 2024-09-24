DROP TABLE if EXISTS doomsday."customer_data";
CREATE TABLE doomsday."customer_data" AS
WITH latest_record_asset AS (
    SELECT
        subscription_id,
        customer_id,
        capital_source_name,
        ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY "date" DESC) AS row_num
    FROM
        master.asset_historical
    WHERE
        capital_source_name IS NOT NULL
)
SELECT DISTINCT
    sub.subscription_id,
    sub.customer_id,
    asset.capital_source_name,
    concat(cp.first_name,cp.last_name) as customer_name,
    cp.first_name,
    cp.last_name,
    cp.email as customer_email
FROM
    master.subscription sub
LEFT JOIN
    latest_record_asset asset
    ON sub.subscription_id = asset.subscription_id
LEFT JOIN
	ods_data_sensitive.customer_pii cp
	ON sub.customer_id = cp.customer_id
WHERE
    sub.status = 'ACTIVE'
    AND asset.row_num = 1;

DELETE FROM doomsday.customer_data_historical
WHERE DATE = CURRENT_DATE - 1;

INSERT INTO doomsday.customer_data_historical
SELECT subscription_id,
customer_id,
capital_source_name,
customer_name,
first_name,
last_name,
customer_email,
CURRENT_DATE - 1 as date
FROM doomsday."customer_data";

COMMIT;
