--delete empty row
DELETE FROM staging.tradedoubler
WHERE programm_country IS NULL OR programm_country  = '' OR LEN(programm_country) >=3;

--refresh table
INSERT INTO marketing.affiliate_tradedoubler_submitted_orders
SELECT 
	a.programm_country,
	CASE WHEN a.visit_time ='' THEN NULL ELSE TO_TIMESTAMP(TO_TIMESTAMP(a.visit_time, 'MM/DD/YY HH:MI:SS AM'), 'YYYY-MM-DD HH24:MI:SS:MS') END AS visit_time,
    CASE WHEN a.transaction_time ='' THEN NULL ELSE TO_TIMESTAMP(TO_TIMESTAMP(a.transaction_time, 'MM/DD/YY HH:MI:SS AM'), 'YYYY-MM-DD HH24:MI:SS:MS') END AS transaction_time,
    -- (CASE WHEN a.visit_time ='' THEN NULL ELSE TO_CHAR(a.visit_time::TIMESTAMP, 'YYYY-MM-DD HH12:MI:SS') END)::TIMESTAMP AS visit_time,
    -- (CASE WHEN a.transaction_time ='' THEN NULL ELSE TO_CHAR(a.transaction_time::TIMESTAMP, 'YYYY-MM-DD HH12:MI:SS') END)::TIMESTAMP AS transaction_time,
	split_part(a.order_id,'_',1) as order_id,
	a.event_id,
	a.website_name,	
	REPLACE(a.order_amount,',','.')::FLOAT order_amount,	
	REPLACE(a.commision_amount,',','.')::FLOAT commision_amount,
	CURRENT_DATE loaded_at 
FROM staging.tradedoubler a
	LEFT JOIN marketing.affiliate_tradedoubler_submitted_orders b USING (order_id)
WHERE b.order_id IS NULL;
