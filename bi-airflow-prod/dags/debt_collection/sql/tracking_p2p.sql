DROP TABLE IF EXISTS  dc_union_;
CREATE TEMP TABLE dc_union_ AS
SELECT --GERMANY
	customer_id::INT,
	contact_date::DATE ,
	contact_type  ,
	promised_date::DATE ,
	REPLACE(promised_amount,',','')::DECIMAL(10,2) AS promised_amount,
	CASE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (0,1) THEN  dateadd('DAY',4,promised_date::DATE)::DATE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (2,3,4,5) THEN  dateadd('DAY',6,promised_date::DATE)::DATE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (6) THEN  dateadd('DAY',5,promised_date::DATE)::DATE
	END AS promised_date_adjusted,
	agent,
	'Germany' AS country
FROM
	staging_airbyte_bi.debt_collection_de
UNION ALL
SELECT --AUSTRIA
	customer_id::INT,
	contact_date::DATE ,
	contact_type  ,
	promised_date::DATE ,
	REPLACE(promised_amount,',','')::DECIMAL(10,2) AS promised_amount,
	CASE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (0,1) THEN  dateadd('DAY',4,promised_date::DATE)::DATE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (2,3,4,5) THEN  dateadd('DAY',6,promised_date::DATE)::DATE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (6) THEN  dateadd('DAY',5,promised_date::DATE)::DATE
	END AS promised_date_adjusted,
	agent,
	'Austria' AS country
FROM
	staging_airbyte_bi.debt_collection_at
UNION ALL
SELECT --SPAIN
	customer_id::INT,
	contact_date::DATE ,
	contact_type  ,
	promised_date::DATE ,
	REPLACE(promised_amount,',','')::DECIMAL(10,2) AS promised_amount,
	CASE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (0,1) THEN  dateadd('DAY',4,promised_date::DATE)::DATE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (2,3,4,5) THEN  dateadd('DAY',6,promised_date::DATE)::DATE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (6) THEN  dateadd('DAY',5,promised_date::DATE)::DATE
	END AS promised_date_adjusted,
	agent,
	'Spain' AS country
FROM
	staging_airbyte_bi.debt_collection_es
UNION ALL
SELECT --NETHERLANDS
	customer_id::INT,
	contact_date::DATE ,
	contact_type  ,
	promised_date::DATE ,
	REPLACE(promised_amount,',','')::DECIMAL(10,2) AS promised_amount,
	CASE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (0,1) THEN  dateadd('DAY',4,promised_date::DATE)::DATE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (2,3,4,5) THEN  dateadd('DAY',6,promised_date::DATE)::DATE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (6) THEN  dateadd('DAY',5,promised_date::DATE)::DATE
	END AS promised_date_adjusted,
	agent,
	'Netherlands' AS country
FROM
	staging_airbyte_bi.debt_collection_nl
UNION ALL
SELECT --US
	customer_id::INT,
	contact_date::DATE ,
	contact_type  ,
	promised_date::DATE ,
	REPLACE(promised_amount,',','')::DECIMAL(10,2) AS promised_amount,
	CASE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (0,1) THEN  dateadd('DAY',4,promised_date::DATE)::DATE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (2,3,4,5) THEN  dateadd('DAY',6,promised_date::DATE)::DATE
		WHEN DATE_PART('WEEKDAY',promised_date::DATE) IN (6) THEN  dateadd('DAY',5,promised_date::DATE)::DATE
	END AS promised_date_adjusted,
	agent,
	'United States' AS country
FROM
	staging_airbyte_bi.debt_collection_us
;

DROP TABLE IF EXISTS  dm_debt_collection.tracking_p2p;
CREATE TABLE dm_debt_collection.tracking_p2p AS
WITH non_paid_payment AS (
    SELECT
    dc.customer_id,
    dc.contact_date,
    dc.contact_type,
    dc.promised_date,
    dc.promised_date_adjusted,
    dc.agent,
    MIN(sp2.due_date)::DATE AS min_non_paid_due_date,
    MIN(ap.due_date)::DATE AS min_non_paid_due_date_asset
    FROM dc_union_ dc
    LEFT JOIN master.subscription_payment sp2
    ON dc.customer_id =sp2.customer_id::INT
    AND sp2.due_date::DATE <= dc.contact_date::date 
    AND COALESCE(sp2.paid_date, '9999-12-31'::DATE) >= dc.contact_date::date
    LEFT JOIN master.asset_payment ap
    ON dc.customer_id =ap.customer_id::INT
    AND ap.due_date::DATE <= dc.contact_date::date 
    AND COALESCE(ap.paid_date, '9999-12-31'::DATE) >= dc.contact_date::date
    GROUP BY 1,2,3,4,5,6
) 
, sub_payments AS (
    SELECT
    dc.customer_id,
    dc.contact_date,
    dc.contact_type,
    c.customer_type,
    dc.promised_date,
    dc.promised_date_adjusted,
    dc.promised_amount,
    dc.agent,
    dc.country,
    npp.min_non_paid_due_date AS min_non_paid_due_date,
    MAX(CASE WHEN sp.amount_paid IS NOT NULL THEN sp.paid_date END)::DATE AS max_paid_date,
    COALESCE(SUM(sp.amount_paid),0) AS paid_amount
    FROM dc_union_ dc
    LEFT JOIN master.subscription_payment sp
    ON dc.customer_id =sp.customer_id::INT
    AND sp.paid_date BETWEEN dc.contact_date::date AND dc.promised_date_adjusted
    LEFT JOIN non_paid_payment npp
    ON dc.customer_id = npp.customer_id
    AND dc.agent = npp.agent
    AND dc.contact_date = npp.contact_date
    AND dc.promised_date = npp.promised_date
    AND dc.promised_date_adjusted = npp.promised_date_adjusted
    LEFT JOIN master.customer c
    ON c.customer_id=dc.customer_id
    GROUP BY 1,2,3,4,5,6,7,8,9,10
) 
, asset_payments AS (
    SELECT
    dc.customer_id,
    dc.contact_date,
    dc.contact_type,
    c.customer_type,
    dc.promised_date,
    dc.promised_date_adjusted,
    dc.promised_amount,
    dc.agent,
    dc.country,
    npp.min_non_paid_due_date_asset AS min_non_paid_due_date,
    MAX(CASE WHEN ap.amount_paid IS NOT NULL THEN ap.paid_date END)::DATE AS max_paid_date,
    COALESCE(SUM(ap.amount_paid),0) AS paid_amount
    FROM dc_union_ dc
    LEFT JOIN master.asset_payment ap
    ON dc.customer_id =ap.customer_id::INT
    AND ap.paid_date BETWEEN dc.contact_date::date AND dc.promised_date_adjusted
    LEFT JOIN non_paid_payment npp
    ON dc.customer_id = npp.customer_id
    AND dc.agent = npp.agent
    AND dc.contact_date = npp.contact_date
    AND dc.promised_date = npp.promised_date
    AND dc.promised_date_adjusted = npp.promised_date_adjusted
    LEFT JOIN master.customer c
    ON c.customer_id=dc.customer_id
   WHERE TRUE 
   --AND npp.min_non_paid_due_date_asset IS NOT NULL 
   GROUP BY 1,2,3,4,5,6,7,8,9,10    
) 
SELECT 
s.customer_id,
s.contact_date,
s.contact_type,
s.customer_type,
s.promised_date,
s.promised_date_adjusted,
s.promised_amount,
s.agent,
s.country,
GREATEST (s.max_paid_date,a.max_paid_date) AS paid_date_,
GREATEST (s.min_non_paid_due_date, a.min_non_paid_due_date) AS min_non_paid_due_date,
SUM(s.paid_amount + COALESCE(a.paid_amount,0)) AS paid_amount_,
CASE
    WHEN (s.promised_amount::DECIMAL(30,2) = paid_amount_::DECIMAL(30,2) OR s.promised_amount::DECIMAL(30,2) < paid_amount_::DECIMAL(30,2) ) AND paid_amount_ <>0 THEN 'Kept'
    WHEN s.promised_amount::DECIMAL(30,2) > paid_amount_::DECIMAL(30,2) AND paid_amount_::DECIMAL(30,2)<>0  THEN 'Partially Kept'
    WHEN paid_amount_::DECIMAL(30,2)= 0  AND s.promised_date_adjusted>CURRENT_DATE THEN 'Pending'
    WHEN paid_amount_::DECIMAL(30,2)= 0  AND s.promised_date_adjusted<=CURRENT_DATE THEN 'Broken' 
END AS promised_status
FROM sub_payments s
LEFT JOIN asset_payments a 
ON s.customer_id = a.customer_id 
AND s.contact_date = a.contact_date
AND s.contact_type = a.contact_type
AND s.customer_type= a.customer_type
AND s.promised_date= a.promised_date
AND s.promised_date_adjusted= a.promised_date_adjusted
AND s.promised_amount = a.promised_amount
AND s.agent = a.agent
AND s.country = a.country
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
;


DROP TABLE IF EXISTS  dm_debt_collection.p2p_payments_collected;
CREATE TABLE dm_debt_collection.p2p_payments_collected AS
WITH payments AS ( 
    SELECT 
        DISTINCT sp.payment_id,
        dc.country AS country_name,
        dc.customer_id,
        COALESCE (sp.amount_paid,0) AS amount_paid,
        sp.paid_date
    FROM dc_union_ dc
        LEFT JOIN master.subscription_payment sp
            ON dc.customer_id=sp.customer_id
            AND sp.paid_date BETWEEN dc.contact_date::date AND dc.promised_date_adjusted
    UNION ALL
    SELECT 
        DISTINCT ap.asset_payment_id AS payment_id,
        dc.country AS country_name,
        dc.customer_id,
        COALESCE (ap.amount_paid,0) AS amount_paid,
        ap.paid_date AS paid_date
    FROM dc_union_ dc
        LEFT JOIN master.asset_payment ap
            ON dc.customer_id = ap.customer_id
            AND ap.paid_date BETWEEN dc.contact_date::date AND dc.promised_date_adjusted
) 
SELECT 
    customer_id,
    country_name,
    paid_date::DATE AS paid_date,
    SUM(amount_paid) AS paid_amount
FROM payments 
WHERE paid_date IS NOT NULL 
GROUP BY 1,2,3;

GRANT SELECT ON dm_debt_collection.p2p_payments_collected TO tableau;
GRANT SELECT ON dm_debt_collection.tracking_p2p TO tableau;
