DROP TABLE IF EXISTS ods_production.order_decline_reason;

CREATE TABLE ods_production.order_decline_reason AS
WITH manual_review_eu_clean_up AS 
(
     SELECT 
        order_id,
        result,
        result_reason,
        result_comment,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at, updated_at, created_date) as rowno
    FROM stg_curated.risk_internal_eu_manual_review_result_v1
    WHERE result IN ('APPROVED', 'DECLINED')
)
, manual_review_us_clean_up AS
(
	select
		order_id,
		status as decision,
		reason,
		comments as comment,
		ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at, updated_at, consumed_at) AS rowno
	from
		stg_curated.risk_internal_us_risk_manual_review_order_v1
)
, mr_reason AS 
(
	select
		order_id,
		"result" AS decision,
		result_reason AS reason,
		result_comment AS comment
	FROM manual_review_eu_clean_up
	WHERE rowno = 1
	UNION
	SELECT
		order_id,
		decision,
		reason,
		comment
	FROM manual_review_us_clean_up us
	WHERE rowno = 1
		AND NOT EXISTS (SELECT NULL FROM manual_review_eu_clean_up eu WHERE eu.order_id = us.order_id)
),
auto_decline_reason_eu_cleanup as 
(
select distinct 
	to_date(updated_at, 'YYYY-MM-DD')::date as updated_at,
	outcome_namespace,
	outcome_message,
	decision_reason,
	to_date(created_at, 'YYYY-MM-DD')::date as created_at,
	order_id,
	customer_id,
	outcome_is_final,
	ROW_NUMBER() OVER(partition by order_id order by to_date(updated_at, 'YYYY-MM-DD') desc ) as row_num
from s3_spectrum_kafka_topics_raw.risk_eu_order_decision_final_v1
where to_date(created_at, 'YYYY-MM-DD')::date >= '2022-10-01'
),
auto_decline_reason_us_cleanup AS
(
SELECT DISTINCT
	to_date(drc.updated_at, 'YYYY-MM-DD')::date as updated_at,
	drc.outcome_namespace,
	dr.message AS outcome_message, -- 
	drc.decision_reason,
	to_date(drc.created_at, 'YYYY-MM-DD')::date as created_at,
	drc.order_id,
	drc.customer_id,
	drc.outcome_is_final,
	ROW_NUMBER() OVER(partition by drc.order_id order by to_date(drc.updated_at, 'YYYY-MM-DD') desc ) as row_num
FROM stg_curated.risk_us_order_decision_compact_v1 drc
LEFT JOIN stg_curated.risk_internal_us_risk_decision_result_v1 dr  -- DRC IS missing outcome_message ("message")
	ON drc.order_id = dr.order_id 
),
auto_decline_reasons_all AS
(
SELECT
	order_id, 
	outcome_message, 
	decision_reason 
FROM auto_decline_reason_eu_cleanup 
WHERE row_num = 1
UNION
SELECT
	order_id, 
	outcome_message, 
	decision_reason 
FROM auto_decline_reason_us_cleanup us
WHERE row_num = 1
	AND NOT EXISTS (SELECT NULL FROM auto_decline_reason_eu_cleanup eu WHERE eu.order_id = us.order_id)
),
auto_reason_new as (
	select 
	*,
	case 
		when decision_reason ilike '%failed identity number%' then 'Failed Identity Number'
		when outcome_message ilike '%company%' or decision_reason ilike '%company%' then 'B2B Declines'
		when outcome_message ilike 'blacklist%' or decision_reason ilike '%blacklist%' then 'Blacklist'
		when outcome_message ilike 'blacklist%' or decision_reason ilike '%blacklist%' then 'Blacklist'
		when outcome_message = 'Temporary Blacklist' then 'Temporary Blacklist'
		when outcome_message in ('Offline cool-off', 'High Market Value','High Risk Categories (Weekly)','High Risk Velocity') 
			or decision_reason ilike '%excessive order behaviour%' then 'Excessive Order Behaviour'
		when outcome_message in ('Failed ID Verification','Rule failed: transactions count < 10 #BAS', 'Rule failed: seizures sum >= 10 #BAS','Mismatching Documents',
		'Rule failed: names mismatch #BAS', 'Rule failed: seizures count >= 2 #BAS', 'Rule failed: Null transactions count #BAS') 
			or decision_reason = 'Failed ID Verification' then 'Failed ID Verification'
		when outcome_message = 'Outstanding Amount' 
			or outcome_message ilike 'outstanding%' or decision_reason ilike 'outstanding%'  or decision_reason ilike 'failed recurring payments%'
			then 'Failed Recurring Payments'
		when outcome_message = 'ML Declined' then 'Model Decline'
		when outcome_message = 'ML Declined' then 'Model Decline'
		when outcome_message ilike 'ssn matches%' or outcome_message ilike 'dangerous%' or outcome_message ilike 'ip matches with%' 
			or outcome_message ilike 'pytank red flag%' or decision_reason ilike 'dangerous%' or decision_reason ilike 'multiple account%'
			then 'Multiple Accounts'
		when outcome_message ilike '%overlimit%' or decision_reason ilike '%overlimit%' then 'Overlimit'
		when outcome_message ilike '%high risk ca%' or decision_reason ilike '%high risk ca%' OR outcome_message ilike '%market value threshold%' or decision_reason ilike '%market value threshold%'
			or 	outcome_message ilike '%cart size%' or decision_reason ilike '%cart size%' or decision_reason ilike '%high risk asset%'
		then 'Suspicious Activity'
		when outcome_message in ('Ekata IDV Decline Rule', 'Ekata Network Decline Rule','Phone Code Hard Mismatch','Invalid Data','Mismatching Documents','Not allowed offline','Phone Code Soft Mismatch','Unsopported Shipping Country: ES','Unsopported Shipping Country: AT','Wrong Address',
					'Suspicious Data') or decision_reason in ('Suspicious Data','Mismatching Data','Unsupported Shipping Country','Not Allowed Offline','Phone Code Soft Mismatch','Mismatching Data') 
					then 'Suspicious Data'
		when outcome_message ilike 'expired id%' or outcome_message ilike 'expired bas%' OR decision_reason ilike 'uncompleted id%'  then 'Uncompleted ID Verification'
		when outcome_message ilike 'credit policy%' or outcome_message ilike 'insufficient credit scores' or decision_reason ilike 'insufficient credit scores%' 
			or outcome_message in ('Equifax hard knockout','Schufa/Crif hard knockout','Schufa M hard knockout','Negative Customer Label: credit default','Positive income method #BAS','Rule failed: maximum soft rules count > 1 #BAS','Unverified offline knockout','Negative Customer Label: fraud','Focum Legal Guardianship'
			'Seon Email hard knockout','Schufa M offline knockout','Experian hard knockout','Equifax hard knockout')
			or outcome_message ilike 'negative customer label:%'
			or decision_reason ilike 'negative ds label'
			or  decision_reason ilike 'schufa high risk'
			or decision_reason ilike 'burgel/boniversum/experian high risk'
			then 'Insufficient Credit Scores'
		when decision_reason ilike 'other%' then 'Others'
		--when outcome_message = 'MR Declined' then 'MR Declined'
		else 'Others'
		end as reason
	from 
	auto_decline_reasons_all
)
SELECT
    DISTINCT o.order_id,
    o.created_date,
    case 
    when mr_reason.order_id is not null then 'Manual Review'
    else 'Auto'
    end as decision_type,
    CASE
    when o.created_date >= '2022-10-01' and mr_reason.order_id is null then ar.reason
    ---added by basri
    when mr_reason.reason in ('Blacklisted','Excessive Order Behaviour', 'Failed ID Verification', 'Failed Recurring Payments', 'Multiple Accounts', 'Overlimit', 'Suspicious Activity', 'Suspicious Data','Others') then mr_reason.reason
	    when mr_reason.reason = 'Customer Overlimit' then 'Overlimit'
	    when mr_reason.reason in ('Other Reason', 'Others','Other Reasons','Other', 'Other reason', 'Oher Reason', 'Otheer Reason', '#REF!', 'generic') or mr_reason.reason is null then 'Others'
	    when mr_reason.reason in ('Credit Score','Insufficient Credit Scores', 'Burgel/Boniversum/Experian High Risk', 'Schufa High Risk') or mr_reason.reason like '%PreciseID%' or mr_reason.reason like '%FICO%' or left(mr_reason.reason, 20) = 'ExperianCreditReport' or left(mr_reason.reason, 13)= 'ExperianScore' then 'Insufficient Credit Score'
	    when mr_reason.reason in ('Outstanding amount', 'OUTSTANDING BALANCE ', 'failed recurring payments','Failed Reccuring Payments') or mr_reason.reason like '%Payments%' then 'Failed Recurring Payments'
	    when mr_reason.reason = 'Blacklist' then 'Blacklisted'
	    when mr_reason.reason ='ID verification rule' or left(mr_reason.reason, 15) ='ID verification' then 'Failed ID Verification'
	    when mr_reason.reason in ('EXCESSIVE ORDERS') then 'Excessive Order Behaviour'
	-------
    WHEN o.store_country = 'United States' AND coalesce(mr_reason.reason,'') <> '' AND regexp_count(mr_reason.reason, '[[:digit:]]') < 1
    	THEN (case when mr_reason.reason IN ('CARD MISMATCH','payments') THEN 'Suspicious Data' else mr_reason.reason end)
   	--Blacklisted
    WHEN cs.is_blacklisted IS True
          OR (
              LOWER(os.order_scoring_comments) LIKE ('%blacklist%')
              OR LOWER(o.declined_reason) LIKE ('%blacklist%')
             )
            THEN 'Blacklisted'
    --ML
    WHEN os.order_scoring_comments LIKE '%DECLINED by ML MODEL%'
          OR os.score > os.decline_cutoff
          --OR ABS(os.anomaly_score) > 3
          OR os.order_scoring_comments LIKE '%ML decline cutoff%'
          OR os.order_scoring_comments LIKE '%ML Declined%'
            THEN 'Model Decline'
    --Failed IDV
    WHEN LOWER(os.order_scoring_comments) LIKE '%id verification failed%'
          OR mr_reason.reason = 'Failed ID Verification'
          OR o.declined_reason = 'Failed ID Verification'
          OR os.verification_state = 'failed'
          OR os.order_scoring_comments = 'Ekata IDV Decline Rule'
          OR os.order_scoring_comments ilike '%Age lower then 18%'
            THEN 'Failed ID Verification'
	--Uncompleted ID Verification
        WHEN LOWER(os.order_scoring_comments) LIKE '%pending id verification%'
          OR os.verification_state IN  ('not_started', 'in_progress')
          or o.declined_reason ilike '%pending id verification%'
          or os.order_scoring_comments ilike '%Expired ID Verification%'
            THEN 'Uncompleted ID Verification'
     --Failed Recurring Payments
        WHEN os.order_scoring_comments ILIKE '%failed recurrent payments:%'
          OR mr_reason.reason ILIKE '%failed recurring payments%'
          OR o.declined_reason ILIKE '%failed recurring payments%'
          OR os.order_scoring_comments ILIKE '%Recurrent failed Payments:%'
          OR mr_reason.reason ILIKE '%outstanding amount%'
          OR mr_reason.comment ILIKE '%outstanding amount%'
          OR o.declined_reason ILIKE '%outstanding amount%'
          OR os.recurrent_failed_payments > 0
          or os.order_scoring_comments like '%outstanding amount%'
            THEN 'Failed Recurring Payments'
    --Suspicious Data
        WHEN mr_reason.reason = 'Suspicious Data'
        or o.declined_reason = 'Suspicious Data'
            THEN 'Suspicious Data'
    --Suspicious Activity
        WHEN mr_reason.reason = 'Suspicious Activity'
            THEN 'Suspicious Activity'   
     ----Insufficient Credit Scores
        WHEN cs.schufa_class IN ('O','P')
          OR cs.burgel_score >= 3
          OR (o.shippingcountry = 'Austria' AND cs.burgelcrif_score_decision = 'RED')
          --OR cs.experian_score < 742
          OR cs.focum_score <= 703
          --OR cs.delphi_score IN ('H', 'I')
          OR cs.equifax_score < 100
		  OR lower(os.scoring_reason) LIKE '%experian hard knockout%'
          OR os.scoring_reason LIKE ('%High risk customer - Bad Schufa score%')
          OR os.order_scoring_comments LIKE '%High risk customer - Bad Schufa score%'
          OR os.order_scoring_comments LIKE '%Credit Policy Declined%'
          OR os.scoring_reason LIKE '%High risk customer - Bad Bürgel score%'
          OR os.order_scoring_comments LIKE '%High risk customer, Bad Burgel score%'
          OR os.scoring_reason LIKE '%ExperianScore: Very high risk%'
          OR os.order_scoring_comments IN ('High Bürgel score', 'High risk area >= 0.5')
          OR os.scoring_reason LIKE 'Bürgel score unknown and  Verita score is bad:%'
          OR cs.customer_scoring_result IN ('Very high risk: RED')
          OR mr_reason.reason iLIKE '%high risk%'
          OR o.declined_reason iLIKE '%high risk%'
          OR mr_reason.reason IN ('Burgel/Boniversum/Experian High Risk', 'High Risk Area')
          OR (os.score >= 0.6 AND cs.focum_score BETWEEN 703 AND 898)
          OR os.order_scoring_comments LIKE 'Note: I' 
          OR os.order_scoring_comments LIKE 'Note: H' 
          OR os.order_scoring_comments LIKE '%Focum/ML matrix%' 
          OR os.order_scoring_comments LIKE '%Focum Legal Guardianship%'
          OR os.order_scoring_comments LIKE '%PreciseID knockout test%'
          OR os.order_scoring_comments LIKE '%PreciseID knockout%'
          OR o.declined_reason LIKE '%PreciseID knockout%'
          OR o.declined_reason LIKE '%Insufficient Credit Scores%'
          or order_scoring_comments = 'Focum Score: 0'
            THEN 'Insufficient Credit Scores'
		--Excessive Order Behaviour
        WHEN 
        mr_reason.reason = 'Excessive Order Behaviour'
        or o.declined_reason = 'Excessive Order Behaviour'
            THEN 'Excessive Order Behaviour'
		--Multiple Accounts
        WHEN 
	        mr_reason.reason = 'Multiple Accounts'
	        OR fraud_type LIKE '%NON_UNIQUE_PHONE_NUMBER%'
	        OR os.order_scoring_comments LIKE '%Similar accounts%'
	        OR os.order_scoring_comments LIKE '%Dangerous Matches%'
	        OR os.order_scoring_comments LIKE '%Multiple accounts%'
	        OR mr_reason.reason = 'Multiple Accounts'
	        OR cs.accounts_cookie > 0
	        OR os.similar_customers_count > 0
	        OR os.order_scoring_comments LIKE '%ssn matches with%'
	        or os.order_scoring_comments LIKE '%Number of shared sessions%'
         THEN 'Multiple Accounts'
		--Country not supported
        WHEN os.order_scoring_comments IN ('Country not supported')
          OR os.order_scoring_comments LIKE '%Different country codes:%'
          OR os.order_scoring_comments LIKE '%Country codes not equal:%'
          OR os.order_scoring_comments LIKE '%Country Mismatch%'
            THEN 'Country not supported'
		--Unknown identity
          WHEN os.verification_state != 'verified'
           AND (os.scoring_reason LIKE 'Address unknown, person unknown'
                OR os.scoring_reason LIKE '%verita person is unknown or dead%'
                OR os.scoring_reason IN ('Reject because fake identity')
                OR cs.customer_scoring_result IN ('Reject because unverified identity',
                                                  'Reject because fake identity')
                OR os.order_scoring_comments LIKE '%Risk undefined%'
                OR os.scoring_reason LIKE '%Burgel does not know the person%'
                OR os.scoring_reason LIKE '%Bürgel dos not know the person%'
                OR os.scoring_reason LIKE '%Bad or undefined score%')
                or mr_reason.reason LIKE '%ExperianCreditReportScore: Undefined Risk:%'
                OR os.order_scoring_comments iLIKE '%experianscore:%undefined%'
                OR os.order_scoring_comments iLIKE '%score%undefined%'
                OR os.order_scoring_comments iLIKE 'Address unknown, person unknown'
                OR os.order_scoring_comments iLIKE '%Reject because fake identity%'
                OR os.order_scoring_comments iLIKE '%Risk undefined: 0%'
            THEN 'Unknown identity'
		--Overlimit
        WHEN LOWER(os.order_scoring_comments) LIKE '%over subscription limit%'
          OR mr_reason.reason IN ('Customer Overlimit', 'Insufficient Payment History')
          OR os.order_scoring_comments LIKE '%Overlimit Order%'
          OR o.declined_reason LIKE '%Overlimit Order%'
          OR o.declined_reason iLIKE '%overlimit%'
          OR o.declined_reason iLIKE  '%over subscription limit%'
          OR os.order_scoring_comments iLIKE '%overlimit%'
          OR os.order_scoring_comments iLIKE '%zero subscription limit%'
         THEN 'Overlimit'
		--Others
		WHEN m.decline_reason is not null THEN m.decline_reason
        ELSE 'Others'
    END AS decline_reason_new
FROM ods_production."order" AS o
LEFT JOIN ods_production.order_scoring AS os ON o.order_id = os.order_id
LEFT JOIN mr_reason ON o.order_id = mr_reason.order_id
LEFT JOIN ods_production.customer_scoring AS cs ON o.customer_id = cs.customer_id
LEFT JOIN ods_production.order_decline_reason_mapping m on lower(os.order_scoring_comments) like lower(m.comment_txt)
left JOIN auto_reason_new ar on ar.order_id = o.order_id
WHERE o.status IN ('DECLINED','MANUAL REVIEW')
;

--grant select on ods_production.order_decline_reason to akshay_shetty;

GRANT SELECT ON ods_production.order_decline_reason TO tableau;
