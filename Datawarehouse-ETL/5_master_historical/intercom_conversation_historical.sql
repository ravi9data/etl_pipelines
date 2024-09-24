--snapshot of everyday
DELETE FROM master.intercom_conversation_historical
WHERE date = current_date;

insert into master.intercom_conversation_historical
SELECT 
id,
created_at,
updated_at,
waiting_since,
snoozed_until,
intercom_customer_id,
delivered_as,
source_id,
redacted,
author_type,
author_id,
fc_reply_at,
fc_reply_type,
fc_url,
"open",
state,
read,
priority,
sla_name,
sla_status,
market,
first_level,
second_level,
third_level,
fourth_level,
team_participated,
teams_count,
admin_assignee_id,
team_assignee_id,
team_assignee_name,
time_to_assignment,
time_to_admin_reply,
time_to_first_close,
time_to_last_close,
median_time_to_reply,
first_assignment_at,
first_admin_reply_at,
first_close_at,
last_assignment_at,
last_assignment_admin_reply_at,
last_contact_reply_at,
last_admin_reply_at,
last_close_at,
last_closed_by_id,
count_reopens,
count_assignments,
count_conversation_parts,
total_count,
conversation_rating,
conversation_remark,
conversation_rating_created_at,
current_date as date,
CURRENT_TIMESTAMP as snapshot_time
FROM ods_data_sensitive.intercom_first_conversation;  
--removing duplicates
