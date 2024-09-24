DROP TABLE IF EXISTS scratch.user_encoded_ids;
CREATE TABLE scratch.user_encoded_ids AS

WITH datadog_users AS (
SELECT distinct user_id AS customer_id
FROM atomic.events 
WHERE useragent like '%Datadog%'
  AND user_id is not null
)

SELECT 
  a.created_at,
  a.customer_id,
  FUNC_SHA1(a.customer_id || 'pepper123') as encoded_id 
FROM master.customer a
LEFT JOIN datadog_users b on a.customer_id = b.customer_id
WHERE b.customer_id IS NULL;
