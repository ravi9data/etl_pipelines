--we will skip the execution if the customer_id did not change
DELETE FROM hightouch_sources.gdpr_consent;

INSERT INTO hightouch_sources.gdpr_consent (customer_id, session_id, session_start, marketing_consent, performance_consent, functional_consent)
WITH anonymous_ids AS (
	SELECT DISTINCT anonymous_id
	FROM segment.sessions_web sw 
	WHERE customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
)
, basis AS (
	SELECT 
		cc.* , 
		sw.session_start ,
		sw.customer_id ,
		marketing_consent || performance_consent || functional_consent AS concat_cookie,
		lag(concat_cookie) OVER (PARTITION BY 1 ORDER BY sw.session_start) AS pre_concat_cookie
	FROM segment.cookie_consent cc 
	INNER JOIN segment.sessions_web sw 
	  ON cc.session_id = sw.session_id 
	INNER JOIN anonymous_ids a 
	  ON a.anonymous_id = sw.anonymous_id 
)
SELECT 
	customer_id,
	session_id,
	session_start,
	marketing_consent,
	performance_consent,
	functional_consent
FROM basis
WHERE concat_cookie <> pre_concat_cookie OR pre_concat_cookie IS NULL 
;
