DROP VIEW IF EXISTS dwh.v_weekly_reporting_order;
CREATE VIEW dwh.v_weekly_reporting_order AS
SELECT 
	o.created_date::date AS created_date ,
	o.order_id,
	o.customer_id ,
	o.submitted_date,
	o.approved_date ,
	o.canceled_date ,
	o.paid_date,
	o.customer_type ,
	o.device ,
	o.initial_scoring_decision ,
	o.cancellation_reason ,
	o.new_recurring ,
	o.order_journey ,
	o.retention_group ,
	o.schufa_class ,
	o.status ,
	o.store_commercial,
	CASE WHEN o.store_commercial like ('%B2B Germany%') AND fm.is_freelancer = 1
    	 THEN 'B2B Freelancers Germany'
    	 WHEN o.store_commercial like ('%B2B International%') AND fm.is_freelancer = 1
      	 THEN 'B2B Freelancers International'
      	 WHEN o.store_commercial like ('%B2B% Germany')
      	 THEN 'B2B Non Freelancers Germany'
      	 WHEN o.store_commercial like ('%B2B% International')
      	 THEN 'B2B Non Freelancers International'
      ELSE o.store_commercial
    END as store_commercial_new,
    CASE WHEN COALESCE(o.customer_type,'No Info') = 'normal_customer' THEN 'B2C'
	     WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fm.is_freelancer = 1 THEN 'B2B freelancer'
	     WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fm.is_freelancer != 1 THEN 'B2B nonfreelancer'
	     WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
		ELSE 'n/a' END AS customer_type_freelancer_split,
    o.store_country ,
  	o.store_label ,
	o.store_short ,
	o.store_type ,
	o.voucher_type ,
    odr.decline_reason_new ,
    oj.order_journey_grouped ,
	CASE WHEN o.new_recurring='RECURRING' then o.retention_group else 'NEW' END AS retention_group_rec,
	CASE WHEN o.device = 'App' then 'APP'
         WHEN o.store_commercial LIKE '%B2B%' then 'B2B'
	  ELSE o.store_short
	END AS store_grouped,
	CASE WHEN o.store_commercial IN ('Partnerships Germany', 'Partnerships International') THEN 'Partnerships - Total'
	     WHEN o.store_commercial IN ('Grover International') THEN 'Grover - EU'
	     WHEN o.store_commercial IN ('Grover Germany') THEN 'Grover - DE'
	     WHEN o.store_commercial IN ('B2B Germany', 'B2B International') THEN 'B2B - Total'
	END AS store_commercial_grouped,
	CASE WHEN store_commercial_grouped IN ('Partnerships - Total')
		  AND o.store_type='offline' then 'Retail - Offline'
	  	 WHEN store_commercial_grouped IN ('Partnerships - Total')
		  AND o.store_type='online' then 'Retail - Online'
	  	 WHEN o.store_short ='Grover International' then o.store_label
	  else store_commercial_grouped
    END AS store_split,
	CASE WHEN store_commercial_grouped = 'Partnerships - Total'
		  AND o.store_type = 'offline' then 'Retail - Offline'
		 WHEN store_commercial_grouped = 'Partnerships - Total'
		  AND o.store_type = 'online' then 'Retail - Online'
		 WHEN store_commercial_grouped ='Grover - EU' then o.store_label
		 WHEN store_commercial_grouped = 'B2B - Total' then o.store_commercial
	  ELSE store_commercial_grouped
	END AS store_split_1,
	ffpr.failed_first_payment_reason ,
	o.completed_orders ,
	o.paid_orders,
	o.declined_orders ,
	o.order_value
FROM master."order" o
LEFT JOIN ods_production.order_decline_reason odr 
  ON o.order_id = odr.order_id 
LEFT JOIN ods_production.order_journey oj 
  ON o.order_id = oj.order_id 
LEFT JOIN ods_production.companies c2 
 on c2.customer_id = o.customer_id
LEFT JOIN dm_risk.b2b_freelancer_mapping fm
 on c2.company_type_name = fm.company_type_name
LEFT JOIN (
		SELECT sq.order_id,
			CASE
				WHEN reason IN ('Insufficient funds.', 'Not enough balance', 'Insufficient Funds', 'AM04:InsufficientFunds', 'MS03:NotSpecifiedReasonAgentGenerated') THEN 'Insufficient funds'
				WHEN reason IN ('Instruct the customer to retry the transaction using an alternative payment method from the customers PayPal wallet. The transaction did not complete with the customers selected payment method.') THEN 'Paypal - Error 10417'
				WHEN reason IN ('The bank refused the transaction.', 'Refused', 'Card refused by the bank networks.') THEN 'The bank refused the transaction.'
				WHEN reason IN ('Blocked Card') THEN 'Blocked Card'
				WHEN reason IN ('referenced transaction is already de-registered') THEN 'Paypal de-register'
				WHEN reason IN ('User''s account is closed or restricted', 'AC04:ClosedAccountNumber', 'Issuer or Cardholder has put a restriction on the card', 'Restricted Card') THEN 'Account is closed or restricted'
				WHEN reason IN ('Agreement was canceled') THEN 'Paypal cancelled agreement'
				WHEN reason IN ('Bank networks failure.') THEN 'Bank networks failure.'
				WHEN reason IN ('Transaction failed but user has alternate funding source') THEN 'Transaction failed but user has alternate funding source'
				WHEN reason IN ('Withdrawal amount exceeded') THEN 'Withdrawal amount exceeded'
				WHEN reason IN ('The parameter "CARDVALIDITYDATE" is invalid.', 'Expired Card', 'Invalid Card Number', 'Invalid card data.') THEN 'Expired / invalid Card'
				WHEN reason IN ('FRAUD', 'Transaction declined by merchant’s rules.','Cardholder has already disputed a transaction.','Transaction declined by merchant.') THEN 'PSP Fraud'
				WHEN reason IN ('Authentication required','Strong customer authentication required by issuer.') THEN 'Authentification issue'
				WHEN reason IN ('The card has been declared as lost.','Call Issuer. Pick Up Card.','The card has been declared as stolen.') THEN 'Lost card - stolen card'
				WHEN reason IN ('Blocked Card') THEN 'Blocked Card'
				ELSE 'Others'				
			END AS failed_first_payment_reason
		FROM (
			SELECT
				row_number()over (partition by order_id order by created_at, failed_date, subscription_id) as row_num,
				order_id,
			 	LEFT(payment_processor_message, CASE WHEN POSITION(';' IN payment_processor_message) = 0 THEN 9999 ELSE POSITION(';' IN payment_processor_message) -1 END) AS reason
			FROM ods_payments.all_failed_first_payments
			WHERE payment_processor_message IS NOT NULL 
		) sq
		INNER JOIN ods_production.order_journey oj ON sq.order_id = oj.order_id AND oj.order_journey_grouped = 'Failed First Payment'
		WHERE row_num = 1
) ffpr
 ON ffpr.order_id = o.order_id
WHERE o.created_date >= DATEADD('week', -7, date_trunc('week', CURRENT_DATE))
  --AND o.submitted_date IS NOT NULL
WITH NO SCHEMA BINDING;  

GRANT SELECT ON dwh.v_weekly_reporting_order TO tableau;
