DROP TABLE IF EXISTS web.session_marketing_mapping;
CREATE TABLE web.session_marketing_mapping
  DISTKEY(session_id)
  SORTKEY(session_id)
AS
SELECT DISTINCT 
  sms.session_id,
  NULL::varchar AS anonymous_id,
  NULL::varchar AS customer_id,
  sms.page_view_start AS session_start,
  NULL::varchar AS marketing_content,
  sms.marketing_medium,
  sms.marketing_campaign,
  sms.marketing_source,
  sms.marketing_term,
  sms.referer_url AS page_referrer,
  sms.marketing_channel
FROM web.session_marketing_mapping_snowplow sms
WHERE sms.page_view_start < '2023-05-01'

UNION 

SELECT 
	seg.session_id::varchar,
	seg.anonymous_id,
	seg.customer_id::varchar,
	seg.session_start,
	seg.marketing_content,
	seg.marketing_medium,
	seg.marketing_campaign,
	seg.marketing_source,
	seg.marketing_term,
	seg.page_referrer,
	seg.marketing_channel
FROM segment.session_marketing_mapping_web seg
WHERE seg.session_start >= '2023-05-01';

GRANT SELECT ON web.session_marketing_mapping TO tableau;
GRANT SELECT ON web.session_marketing_mapping TO redash_growth;
