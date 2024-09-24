WITH conversation_parts_assignements AS (
SELECT 
conversation_id ,
min(CASE WHEN conversation_part_assigned_to_id ='4988456' AND conversation_part_type IN ('assignment','comment','assign_and_unsnooze') THEN conversation_part_created_at END) AS first_card_assigned,
max(CASE WHEN conversation_part_assigned_to_id ='4988456' AND conversation_part_type IN ('assignment','comment','assign_and_unsnooze') THEN conversation_part_created_at END) AS last_card_assigned,
max(CASE WHEN conversation_part_type in ('assignment','comment','assign_and_unsnooze') AND conversation_part_assigned_to_type='team'  THEN  conversation_part_created_at END ) AS last_assigned_team_date,
CASE WHEN last_card_assigned=last_assigned_team_date THEN TRUE ELSE FALSE END AS is_card_assigned
FROM ods_data_sensitive.intercom_conversation_parts icp 
INNER JOIN (SELECT id from ods_data_sensitive.intercom_first_conversation ifc WHERE ifc.team_assignee_name ='Grover Card') ifc 
ON ifc.id=icp.conversation_id 
WHERE TRUE 
--and conversation_id ='95986502557059'
GROUP BY 1)
,intercom_assignments AS(
SELECT 
	ia.conversation_id
	,min(CASE WHEN is_selected_team='Grover Card' THEN ia.created_at END ) first_card_assigned
	,max(CASE WHEN is_selected_team='Grover Card' THEN ia.created_at end) last_card_assigned
	,max(CASE WHEN conversation_part_assigned_to_type='team' THEN ia.created_at end) AS last_assigned_team_date
	,CASE WHEN last_card_assigned=last_assigned_team_date THEN TRUE ELSE FALSE END AS is_card_assigned
FROM ods_operations.intercom_assignments ia 
WHERE TRUE 
--and conversation_id ='95986502557059'
GROUP BY 1)
--,FINAL AS (
SELECT 
ic.id
, ic.created_at
, intercom_customer_id
, COALESCE (ia.first_card_assigned,cpa.first_card_assigned) AS first_card_assigned
, delivered_as
,state
, source_id
, redacted
, author_type
, author_id
, team_participated
, teams_count
, admin_assignee_id
, team_assignee_id
, team_assignee_name
, time_to_assignment
, first_assignment_at
, first_admin_reply_at
, first_close_at
, last_assignment_at
, last_assignment_admin_reply_at
, last_contact_reply_at
, last_admin_reply_at
, last_close_at
, last_closed_by_id
, count_reopens
, count_assignments
, count_conversation_parts
, total_count
, conversation_rating
, conversation_rating_created_at
, admins_list
, admins_list_length
, ic.custom_attributes
, contact_reason
, duplicate
, b2b_contact_reason
, card_contact_reason
, contact_reason_grouped
, customer_id
,COALESCE(ia.is_card_assigned,cpa.is_card_assigned) AS is_card_assigned
,CASE WHEN ia.is_card_assigned=FALSE THEN cpa.is_card_assigned ELSE ia.is_card_assigned END AS is_card_assigned_new
,CASE WHEN last_closed_by_id IN ('1816605','2698520','2698365') and state = 'closed' THEN TRUE ELSE FALSE END AS is_closed_by_bot
left JOIN intercom_assignments ia 
ON ic.id=ia.conversation_id 
left JOIN conversation_parts_assignements cpa
ON ic.id=cpa.conversation_id 
WHERE contact_reason_grouped<> 'Card Outgoing ticket'
ORDER BY first_card_assigned DESC, created_at DESC 
WITH NO SCHEMA binding;



