drop table if exists ods_external.aircall;
create table ods_external.aircall as 
WITH customer AS (
         SELECT DISTINCT replace(replace(replace(replace(replace(COALESCE(c_1.phone_number)::text, ' '::text, ''::text), '+'::text, ''::text), '-'::text, ''::text), '49'::text, ''::text), '0'::text, ''::text) AS phone_based_id,
            c_1.customer_id
           FROM ods_data_sensitive.customer_pii c_1
        )    
        SELECT 
 	c.id,
    c.direction,
    c.started_at,
    c.answered_at,
    c.ended_at,
    c.duration,
    c.raw_digits,
    replace(replace(replace(replace(replace(c.raw_digits::text, ' '::text, ''::text), '+'::text, ''::text), '-'::text, ''::text), '49'::text, ''::text), '0'::text, ''::text) AS phone_based_id,
    COALESCE(u.name, 'not available'::character varying) AS agent_name,
        CASE
            WHEN c.voicemail IS NOT NULL THEN true
            ELSE false
        END AS is_voicemail,
    c1.customer_id
   FROM stg_external_apis.aircall_call c
     LEFT JOIN stg_external_apis.aircall_user u ON c.user_id = u.id
     LEFT JOIN customer c1 ON replace(replace(replace(replace(replace(c.raw_digits::text, ' '::text, ''::text), '+'::text, ''::text), '-'::text, ''::text), '49'::text, ''::text), '0'::text, ''::text) = c1.phone_based_id
  WHERE c.status::text = 'done'::text
  ORDER BY c.started_at DESC;

 drop table ods_external.aircall_subscription_payment;
create table ods_external.aircall_subscription_payment as
with calls as --Joining aircall and subscription payment table and calculating calls kpi
 ( 
 Select 
 	sp1.customer_id
, 	sp1.subscription_id
,	sp1.subscription_payment_id
,	sp1.amount_due
,	sp1.amount_paid
,	sp1.due_date
,	sp1.paid_date
,		MAX(CASE
        	WHEN a1.started_at >= sp1.due_date AND a1.started_at <= COALESCE(sp1.paid_date, (sp1.due_date::date + 90)::timestamp without time zone) THEN (a1.started_at)
            	ELSE NULL::timestamp
            END )AS max_call_date
,		MAX(CASE
            WHEN a1.started_at >= sp1.due_date AND a1.started_at <= COALESCE(sp1.paid_date, (sp1.due_date::date + 90)::timestamp without time zone) AND a1.answered_at IS NOT NULL THEN (a1.started_at)
                ELSE NULL::timestamp
            end) AS max_answered_date,
        MAX(CASE
            WHEN a1.started_at >= sp1.due_date AND a1.started_at <= COALESCE(sp1.paid_date, (sp1.due_date::date + 90)::timestamp without time zone) AND a1.is_voicemail IS NOT NULL THEN (a1.started_at)
                ELSE NULL::timestamp
            end) AS max_voicemail_date
,		count(DISTINCT
            CASE
            WHEN a1.started_at >= sp1.due_date AND a1.started_at <= COALESCE(sp1.paid_date, (sp1.due_date::date + 90)::timestamp without time zone) THEN a1.customer_id
                ELSE NULL::bigint
            END) AS count_calls
,       count(DISTINCT
            CASE
            WHEN a1.started_at >= sp1.due_date AND a1.started_at <= COALESCE(sp1.paid_date, (sp1.due_date::date + 90)::timestamp without time zone) AND a1.answered_at IS NOT NULL THEN a1.customer_id
                ELSE NULL::bigint
            END) AS count_answered_calls
,   	count(DISTINCT
            CASE
            WHEN a1.started_at >= sp1.due_date AND a1.started_at <= COALESCE(sp1.paid_date, (sp1.due_date::date + 90)::timestamp without time zone) AND a1.is_voicemail THEN a1.customer_id
                 ELSE NULL::bigint
            END) AS count_voicemail_calls           
from ods_production.payment_subscription sp1
	left join ods_external.aircall a1 on sp1.customer_id=a1.customer_id
group by 1,2,3,4,5,6,7
)
select 
	c1.*,
	s1.result_debt_collection_contact
--    ,s1.dc_deadline_date
from calls c1
left join ods_production.subscription s1 on c1.subscription_id=s1.subscription_id;


drop table ods_external.aircall_subscription;
create table ods_external.aircall_subscription as 
SELECT 
	c1.id, 
	c1.direction, 
	c1.started_at, 
	c1.answered_at, 
	c1.ended_at, 
	c1.duration, 
	c1.raw_digits, 
	c1.phone_based_id, 
	c1.agent_name,
	c1.is_voicemail, 
	c1.customer_id,
	s1.result_debt_collection_contact,
	cs1.customer_type
FROM ods_external.aircall c1
left join ods_production.subscription s1 on c1.customer_id=s1.customer_id
left join ods_production.customer cs1 on cs1.customer_id=c1.customer_id;

GRANT SELECT ON ods_external.aircall_subscription_payment TO tableau;
GRANT SELECT ON ods_external.aircall_subscription TO tableau;
