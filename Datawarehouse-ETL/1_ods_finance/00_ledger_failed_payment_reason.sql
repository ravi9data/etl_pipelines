DROP TABLE IF EXISTS ods_production.ledger_failed_payment_reason;
CREATE TABLE ods_production.ledger_failed_payment_reason AS
WITH number_sequance AS (
SELECT 
 ordinal
FROM public.numbers
WHERE ordinal < 20
)
,failed_payments AS (
SELECT 
  lcs.id
 ,lcs.latest_movement_id
 ,lcs.latest_movement_created_at_timestamp AS failed_date
 ,CASE
   WHEN lcs.provider_response_clean LIKE '%payload%' 
    THEN JSON_EXTRACT_PATH_TEXT(lcs.provider_response_clean,'payload')
   ELSE provider_response_clean
  END AS payload
 ,COALESCE(NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(payload,'errors'),'error'),''),JSON_EXTRACT_PATH_TEXT(payload,'error')) AS error
 ,JSON_ARRAY_LENGTH(error, TRUE) AS total_error_items  /*total_error_items SHOWS NR OF CASES IF THERE IS AN ARRAY, OTHERWISE IT IS NULL*/
 ,CASE 
   WHEN total_error_items IS NULL 
    THEN error
 	 ELSE JSON_EXTRACT_ARRAY_ELEMENT_TEXT(error, ns.ordinal::INT, TRUE)
 	END AS error_splitted
 ,CASE
	 WHEN IS_VALID_JSON(error_splitted) 
     AND POSITION('code' IN error_splitted) > 0  
    THEN JSON_EXTRACT_PATH_TEXT(error_splitted,'code') 
	 WHEN POSITION('message' IN error_splitted) > 0 
    THEN JSON_EXTRACT_PATH_TEXT(error_splitted,'message') 
	 ELSE NULL
	END AS failed_reason_pre
/* The 'code' field appears in both numeric and text formats. For consistency, in the line below we will use the 'message' info if the code is numeric, such as '2006', which corresponds to the resource ID rss_6921007321402032128. */ 
 ,CASE
	 WHEN LEN(failed_reason_pre)::NUMERIC = 4 
    THEN JSON_EXTRACT_PATH_TEXT(error_splitted,'message') 
	 ELSE failed_reason_pre
	 END AS failed_reason
 ,ROW_NUMBER() OVER (PARTITION BY lcs.id ORDER BY lcs.created_at DESC) rn
FROM ods_production.ledger_curated lcs
  CROSS JOIN number_sequance ns 
WHERE TRUE 
  AND lcs.latest_movement_status = 'FAILED'
  AND ns.ordinal < COALESCE(total_error_items,1)
/*IN SOME CASES THERE ARE 2 ERROR MESSAGES, Eg.  slug = 'F-B6B3C9M6-5'
 *IN SUCH CASES WE TAKE THE FIRST ONE*/  
  AND ns.ordinal = 0
)
SELECT 
  id
 ,latest_movement_id
 ,failed_date
 ,failed_reason
 ,rn
FROM failed_payments
;

GRANT SELECT ON ods_production.ledger_failed_payment_reason TO payments_redash;
