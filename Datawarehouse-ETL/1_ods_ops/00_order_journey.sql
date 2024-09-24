drop table if exists ods_production.order_journey;

create table ods_production.order_journey AS
WITH onfido_eu AS
(
	SELECT
		o.customer_id,
		o.order_id,
		os.verification_state,
		os.onfido_trigger,
		ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY o.submitted_date) AS row_num -- IN CASE dupplicates exist IN Master.ORDER IN the future
	FROM master."order" o
	LEFT JOIN ods_production.order_scoring os 
		ON os.order_id = o.order_id
	WHERE os.onfido_trigger IS NOT NULL
		AND o.store_country != 'United States' 
)
, onfido_us AS
(
	SELECT
		customer_id,
		order_id,
		verification_state,
		ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS row_num
	FROM stg_curated.risk_internal_us_risk_id_verification_request_v1 
)
SELECT		o.order_id,
	 		o.status,
	 		o.store_type,
			o.customer_id,
			o.initial_scoring_decision,
			o.created_date,
			o.submitted_date,
			o.manual_review_ends_at,
			o.approved_date,
			o.paid_date,
			o.canceled_date,
	Case when store_type = 'online' then
		Case when submitted_date is null and store_commercial like '%B2B%' and status = 'DECLINED' then 'Declined by B2B team'
--			 when submitted_date is null and store_type = 'offline' and status = 'DECLINED' then 'Offline Declined before payment method'
--		 	when submitted_date is null and store_type = 'offline' and status = 'APPROVED' then 'Offline Declined after payment method'
			when submitted_date is null and status = 'DECLINED' then 'Declined Others'
			when submitted_date is null then 'Not Submitted'
			when submitted_date< '2019-02-01' then 'Before Manual Review Introduction'
			when submitted_date>= '2019-02-01' and submitted_date < CURRENT_DATE then 
					case when status = 'CANCELLED' and canceled_date>paid_date
						then '4.1 Cancelled After Paid'
						when status = 'PAID' and paid_date is not null and approved_date is not null
						then 'Paid After Approved'
						when paid_date is null and approved_date is not null and canceled_date is null and status = 'FAILED FIRST PAYMENT'
						then '3.3 Payment failed after Approval'
						when paid_date is null and approved_date is not null and canceled_date is null and status != 'FAILED FIRST PAYMENT'
						then '3.4 Stuck in Payment Process'
						WHEN status = 'FAILED FIRST PAYMENT'
						THEN '3.5 Failed First Payment'
						when canceled_date>approved_date and status = 'CANCELLED'
						then '3.2 Cancelled after Approval before Payment'
						when initial_scoring_decision = 'APPROVED' and approved_date is null and status = 'DECLINED'
						then '2.2 Declined after Initially Approved'
						when initial_scoring_decision = 'APPROVED' and canceled_date < manual_review_ends_at and approved_date is null 
						then '2.1 Cancelled during Manual Review - Initially Approved'
						when initial_scoring_decision = 'DECLINED' and approved_date is null and status = 'DECLINED'
						then '2.4 Declined after Initially Declined'
						when initial_scoring_decision = 'DECLINED' and canceled_date < manual_review_ends_at and approved_date is null
						then '2.3 Cancelled during Manual Review -Initially Declined'
						when coalesce(initial_scoring_decision,'')='' and status = 'DECLINED'
						then '0.1 Hard Decline - Bad Schufa/Credit' 
						when coalesce(initial_scoring_decision,'')='' and (canceled_date is not null or status = 'CANCELLED')
						then '0.2 Cancelled before Manual Review process'
						when status = 'MANUAL REVIEW' and initial_scoring_decision = 'DECLINED'
						then '1.2 Manual Review - Initially Declined'
						when  status = 'MANUAL REVIEW' and initial_scoring_decision = 'APPROVED'
						then '1.1 Manual Review - Initially Approved'
						when status = 'MANUAL REVIEW' and manual_review_ends_at is null and submitted_date >= DATEADD(day, -7, current_date)
						then '1.3 Stuck in Manual Review'
						when status = 'PENDING APPROVAL' and initial_scoring_decision = 'DECLINED'
						then '1.5 Pending Approval - Initially Declined'
						when  status = 'PENDING APPROVAL' and initial_scoring_decision = 'APPROVED'
						then '1.4 Pending Approval - Initially Approved'
						when status = 'PENDING APPROVAL'
						then '1.6 Pending Approval'
						when status = 'PAID' and canceled_date is NULL and paid_date<manual_review_ends_at
						then '2.5 Paid before Manual Review ends'
						when status = 'PAID' and paid_date is not null
						then 'Paid'
						when status = 'CANCELLED' and canceled_date> manual_review_ends_at 
						then '3.1 Cancelled - After Manual Review'
						when status = 'CANCELLED' and canceled_date is not null
						then '3.2 Cancelled'
						else 'Error in Order Journey'
						End 
						else '0 Order from Today' 
				End	
					else 
			 case when submitted_date is null and store_type = 'offline' and status = 'DECLINED' then 'Offline Declined before payment method'
		 		  when submitted_date is null and store_type = 'offline' and status = 'APPROVED' then 'Offline Declined after payment method'
			 	  when submitted_date is null and store_type = 'offline'  then 'Offline order Not Submitted'  
				  when status = 'PAID' then 'Offline Order Paid'
				  else 'others'
				end
				End as order_journey,
		case 
			when order_journey like '%Payment failed%' or status = 'FAILED FIRST PAYMENT' then 'Failed First Payment'
			when status = 'PENDING PAYMENT' then 'Pending Payment'
			when order_journey like '%Stuck in Payment%' then 'Stuck in Payment process'
			when order_journey in ('Paid After Approved', 'Offline Order Paid') or status in ('PAID') then 'Paid'
			when order_journey ='0.1 Hard Decline - Bad Schufa/Credit' then 'Hard Decline'
			when order_journey like '%Cancelled%' and order_journey != '4.1 Cancelled After Paid' then 'Cancelled before Paid'
			when order_journey = '4.1 Cancelled After Paid' then 'Cancelled after Paid'
			when order_journey = '2.2 Declined after Initially Approved' then 'Declined after Approved'
			when order_journey = '2.4 Declined after Initially Declined'  
			  or status ='DECLINED' 
			  or order_journey ='1.2 Manual Review - Initially Declined'
			then 'Declined after Declined'
			when order_journey = 'Declined by B2B team' then 'Declined by B2B team'
			when order_journey like '%Offline Declined%' then 'Offline Declined'
			when order_journey like '%Declined Others%' then 'Declined after Declined'--'Declined Others'
			when order_journey = '%Not Submitted%' then 'Not Submitted'
			when status='MANUAL REVIEW' then 'Manual Review'
			else 'Others'
				end order_journey_grouped,
		CASE 
			 WHEN status = 'PAID' THEN 'Paid'
			 WHEN status = 'FAILED FIRST PAYMENT' THEN 'FFP'
			 WHEN status = 'DECLINED' 
			 	AND
			 	(
				bas.id_check_result IN ('NOT_COMPLETE', 'FAILURE')
				OR 
				oeu.verification_state IN ('failed', 'in_progress', 'not_started')	
				OR
				ous.verification_state IN ('failed', 'in_progress', 'not_started')
				)
					THEN 'Declined - BAS/Onfido related'
			 WHEN status = 'DECLINED' THEN 'Declined'
			 WHEN submitted_date IS NULL AND store_type = 'offline' AND status IN ('APPROVED', 'DECLINED') THEN 'Declined'
			 WHEN submitted_date IS NULL THEN 'Not Submitted'
			 WHEN (canceled_date > manual_review_ends_at OR canceled_date > approved_date) /*AND paid_date IS NULL*/ THEN 'Post Approval Cancellations'
			 WHEN paid_date IS NOT NULL AND status = 'CANCELLED' THEN 'Post Approval Cancellations'
			 WHEN (canceled_date < approved_date) OR (approved_date IS NULL AND canceled_date IS NOT NULL) THEN 'Pre-Approval Cancellation'
			 WHEN initial_scoring_decision IN ('DECLINED','APPROVED') AND canceled_date < manual_review_ends_at THEN 'Pre-Approval Cancellation'
			 WHEN COALESCE(initial_scoring_decision,'')='' AND (canceled_date IS NOT NULL OR status = 'CANCELLED') THEN 'Pre-Approval Cancellation'
			 WHEN (paid_date IS NULL AND approved_date IS NOT NULL AND canceled_date IS NULL AND status != 'FAILED FIRST PAYMENT') OR status = 'PENDING PAYMENT' THEN 'Pending Payment'
			 WHEN bas.order_id IS NOT NULL
				AND bas.id_check_result NOT IN ('SUCCESS', 'FAILURE')
						THEN 'Bank Account Snapshot'
			 WHEN (oeu.verification_state IN ('in_progress', 'not_started')
				OR ous.verification_state IN ('in_progress', 'not_started'))
						THEN 'Onfido'
			 WHEN status = 'MANUAL REVIEW' then 'Manual Review'
			 WHEN status IN ('PENDING APPROVAL', 'ON HOLD', 'PENDING PROCESSING') THEN 'Pending'
			 WHEN submitted_date >= CURRENT_DATE THEN 'Pending'	
			 WHEN approved_date IS NULL AND paid_date IS NULL AND canceled_date IS NULL AND status = 'APPROVED' THEN 'Pending'
			 WHEN approved_date IS NULL AND paid_date IS NULL AND canceled_date IS NULL AND status = 'CANCELLED' THEN 'Pre-Approval Cancellation'
			 ELSE 'Others'
		END AS order_journey_mapping_risk
from ods_production.ORDER o
LEFT JOIN dm_risk.bank_acccount_snapshot_results bas
	ON o.order_id = bas.order_id
LEFT JOIN onfido_eu oeu
	ON o.order_id = oeu.order_id
	AND oeu.row_num = 1
LEFT JOIN onfido_us ous
	ON o.order_id = ous.order_id
	AND ous.row_num = 1 ;

GRANT SELECT ON ods_production.order_journey TO tableau;
