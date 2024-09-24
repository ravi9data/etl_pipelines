WITH author_id_pre AS (
SELECT DISTINCT conversation_part_author_id,conversation_part_author_email,
ROW_NUMBER ()OVER (PARTITION BY conversation_part_author_id  ORDER BY conversation_part_created_at DESC )idx
FROM  ods_data_sensitive.intercom_conversation_parts icp 
WHERE conversation_part_author_type='user')
,author_id AS (
SELECT * FROM author_id_pre WHERE idx=1
--AND conversation_part_author_id  ='60ad707c0e9f8b9fc92a99e2'
),FINAL AS (
SELECT DISTINCT ifc.*,
 split_part(author_email,'@',2) AS email_end,
		OR  split_part(author_email,'@',2) ILIKE  '%intercom%' THEN conversation_part_author_email ELSE author_email END AS new_email,
 split_part(new_email,'@',2) AS new_email_end,
 CASE 
 	WHEN card_contact_reason='Card General Questions' Then 'Card General Questions' 
 	WHEN card_contact_reason='Card Grover Cash' Then 'Card Grover Cash' 	
 	WHEN card_contact_reason='Card Personal details' Then 'Card Personal details' 
 	WHEN card_contact_reason='Card Video Identification' Then 'Card Onboarding' 
 	WHEN card_contact_reason='❌ Card / Account Block Unblock' Then 'Card Block/Unblock and Physical issue' 
 	WHEN card_contact_reason='❌ Card Block Unblock' Then 'Card Block/Unblock and Physical issue' 
 	WHEN card_contact_reason='👾 Card Bugs' Then 'Card Issues' 
 	WHEN card_contact_reason='💳 Card SB Outgoing' Then 'Card Outgoing ticket' 
 	WHEN card_contact_reason='💵 Card Grover Cash' Then 'Card Grover Cash' 
 	WHEN card_contact_reason='📂 Card Personal details' Then 'Card Personal details' 
 	WHEN card_contact_reason='📖 Card General Questions' Then 'Card General Questions' 
 	WHEN card_contact_reason='📦 Card Delivery issues' Then 'Card Onboarding' 
 	WHEN card_contact_reason='📸 Card Video Identification' Then 'Card Onboarding' 
 	WHEN card_contact_reason='🔄 Card Reorder' Then 'Card Block/Unblock and Physical issue' 
 	WHEN card_contact_reason='🔓 Card Account Closure' Then 'Account Closure' 
 	WHEN card_contact_reason='🔓. Card GDPR' Then 'Card Personal details' 
 	WHEN card_contact_reason='🕵️ Card Lost, Stolen, Damaged, or Fraud' Then 'Card Block/Unblock and Physical issue' 
 	WHEN card_contact_reason='🗂️  Card Transaction issues' Then 'Card Issues' 
 	WHEN card_contact_reason='🚨 Card Complaints' Then 'Card Issues' 
 	WHEN card_contact_reason='🧾  Card Tax Identification' Then 'Card Personal details'  
 	when card_contact_reason=' ' THEN 'Unknown'
 	ELSE NULL 
 END AS Contact_reason_grouped
FROM ods_data_sensitive.intercom_first_conversation ifc 
LEFT JOIN author_id a 
ON ifc.intercom_customer_id=a.conversation_part_author_id
where team_assignee_name='Grover Card'
)
SELECT 
id
, f.created_at
, intercom_customer_id
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
, f.custom_attributes
, contact_reason
, duplicate
, b2b_contact_reason
, card_contact_reason
, Contact_reason_grouped
,cp.customer_id FROM FINAL f
LEFT JOIN ods_data_sensitive.customer_pii cp 
ON f.new_email=cp.email;
