/*
 * REFERRAL HOST GUEST MAPPING 
 */
DROP TABLE IF EXISTS master_referral.host_guest_mapping;
CREATE TABLE master_referral.host_guest_mapping AS
SELECT
	DISTINCT 
 hr.host_id,
 gs.guest_id,
 gso.order_id,
 gs.transaction_id ,
 gs.campaign_id ,
 hr.host_referred_date,
 gs.guest_created_date ,
 gso.submitted_date,
 gao.approved_date,
 gop.paid_date,
 gcs.Loyalty_contract_start_date,
 gcs.wideruf_date,
 gr.revoked_date,
 gs.guest_country ,
 host_country,
 CASE WHEN hr.first_guest_referred=gs.guest_id  THEN TRUE ELSE FALSE END AS is_first_guest
FROM ods_referral.guest_signup gs 
LEFT JOIN ods_referral.host_referred hr 
	ON gs.host_id =hr.host_id 
LEFT JOIN ods_referral.guest_submitted_order gso 
	ON gso.guest_id =gs.guest_id 
LEFT JOIN ods_referral.guest_approved_order gao 
	ON gao.order_id=gso.order_id 
LEFT JOIN ods_referral.guest_order_paid gop 
	ON gso.order_id=gop.order_id  
LEFT JOIN ods_referral.guest_contract_started gcs 
	ON gcs.order_id =gso.order_id  
LEFT JOIN ods_referral.guest_revoked gr 
	ON gr.order_id=gso.order_id ;

GRANT SELECT ON master_referral.host_guest_mapping TO tableau;
