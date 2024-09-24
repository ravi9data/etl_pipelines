--we will skip the execution if the customer_id did not change
DELETE FROM hightouch_sources.gdpr_communication;

INSERT INTO hightouch_sources.gdpr_communication (customer_id, contact_date, state, delivered_as, contact_reason, contact_reason_detailed)
WITH email_address AS (
	SELECT pii.customer_id, email
	FROM ods_data_sensitive.customer_pii pii
	WHERE pii.customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
)
, user_info AS (
	SELECT DISTINCT e.customer_id, e.email, JSON_EXTRACT_PATH_TEXT(c.author,'id') as author_id
	FROM email_address e
	INNER JOIN ods_intercom.conversations c
		ON e.email = JSON_EXTRACT_PATH_TEXT(c.author,'email')
)
, first_conversation AS (
	SELECT DISTINCT
		a.customer_id,
		c.id AS conversation_id,
	 	c.created_at as contact_date,
	    c.state,
	    c.delivered_as,
		c.contact_reason
	FROM user_info a
	INNER JOIN ods_operations.intercom_first_conversation_snapshot c
		ON a.author_id = c.author_id
	WHERE contact_reason <> ''
)
, conversations AS ( 
	SELECT DISTINCT
		e.customer_id,
		c.conversation_id,
	 	(timestamptz 'epoch' + c.created_at::bigint * interval '1 second') as contact_date,
	    c.state,
	    c.delivered_as,
		REGEXP_REPLACE(c.body, '<[^<>]*>', '') as contact_reason_detailed
	FROM user_info e
	INNER JOIN ods_intercom.conversations c
		ON e.email = JSON_EXTRACT_PATH_TEXT(c.author,'email')
	WHERE contact_reason_detailed NOT LIKE '%Ok %'
)
SELECT
	COALESCE(c.customer_id,f.customer_id) AS customer_id,
 	COALESCE(c.contact_date,f.contact_date) as contact_date,
    COALESCE(c.state,f.state) as state,
    COALESCE(c.delivered_as,f.delivered_as) as delivered_as,
	f.contact_reason,
	c.contact_reason_detailed
FROM first_conversation f
FULL OUTER JOIN conversations c 
  ON f.conversation_id = c.conversation_id