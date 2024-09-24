
WITH redemption AS (
	SELECT 
		customer_id AS redemption_user,
	WHERE event_name='Redemption' 
	GROUP BY 1
	)
,Early_earning_pre AS (
	Select 
		date_trunc('month',event_timestamp::date) as earning_month, 
		customer_id::int as customer_id, 
		COALESCE(sum(CASE when transaction_type = 'PURCHASE' Then amount_transaction *0.03 end),0) as Cash_earned,
		COALESCE(sum(CASE when transaction_type = 'CREDIT_PRESENTMENT' then amount_transaction * 0.03 end),0) as Cash_refunded,
		cash_earned - cash_refunded as Monthly_Cash_Balance,
		CASE WHEN Monthly_Cash_Balance>60 THEN 60 ELSE Monthly_Cash_Balance END AS Monthly_Cash_Balance_limit
where transaction_date < '2022-04-07 00:15:00'
and merchant_category_code not in ('4829','6050','6051','6211','6532','6536','6537',
									'6538','6540','6760','8651','9211','9222','9223','9411',
									'7995','8999','6010','6011','6012')
and amount_transaction > 0.99
Group by 1,2
order by 1 DESC
)
,Early_earning AS( 
	SELECT 
		customer_id::int AS early_earning_user,
		sum(monthly_cash_balance_limit) AS early_earned_cash_card
	FROM Early_earning_pre
	GROUP BY 1)
, Late_earning_pre AS (
	Select 
		date_trunc('month',event_timestamp::date) as earning_month, 
		customer_id::int AS customer_id , 
		cash_earned - cash_refunded as Monthly_Cash_Balance,
		CASE WHEN Monthly_Cash_Balance>60 THEN 60 ELSE Monthly_Cash_Balance END AS Monthly_Cash_Balance_limit
Group by 1,2
order by 1 DESC
)
,Late_earning AS( 
	SELECT 
		customer_id AS late_earning_user,
		sum(monthly_cash_balance_limit) AS late_earned_cash_card
	FROM Late_earning_pre
	GROUP BY 1)
,late_earning_refferal AS (
	SELECT 
		gc.customer_id::int AS earning_user,
	WHERE event_name='Earning'
	AND transaction_type ='add'
	GROUP BY 1)
,earning_sum AS (
	SELECT 
		le.late_earning_user AS earning_user,
		le.late_earned_cash_card AS earned_cash_card,
		0 AS earned_cash_referral
	FROM late_earning le 
	UNION 
	SELECT 
		ee.early_earning_user AS earning_user,
		ee.early_earned_cash_card AS earned_cash_card,
		0 AS earned_cash_referral 
	FROM Early_earning ee 
	UNION 
	SELECT 
		ler.earning_user,
		0 AS earned_cash_card,
		ler.earned_cash_referral AS earned_cash_referral
	FROM late_earning_refferal ler )
,earning AS (
	Select
		earning_user,
		sum(earned_cash_card) AS earned_cash_card,
		sum(earned_cash_referral) AS earned_cash_referral
		FROM earning_sum 
		GROUP BY 1
		)	
,FINAL AS (
SELECT e.earning_user AS customer_id,
	   CASE WHEN earned_cash_card =0 AND earned_cash_referral >0 THEN 'Referral_user'
	  		WHEN earned_cash_card >0 AND earned_cash_referral =0 THEN 'Card_user'
	  		WHEN earned_cash_card >0 AND earned_cash_referral >0 THEN 'Both'
	  		ELSE 'No earning' END User_classification,
	   COALESCE(earned_cash_card,0)+COALESCE(earned_cash_referral,0) AS total_cash_earned,
	   COALESCE(earned_cash_card,0) AS earned_cash_card,
	   COALESCE(earned_cash_referral,0) AS earned_cash_referral,
	   (COALESCE(earned_cash_card,0)*COALESCE(redemption_cash,0))/NULLIF(COALESCE(earned_cash_card,0)+COALESCE(earned_cash_referral,0),0) AS redemption_card,
	  (COALESCE(earned_cash_referral,0)*COALESCE(redemption_cash,0))/NULLIF(COALESCE(earned_cash_card,0)+COALESCE(earned_cash_referral,0),0) AS redemption_referral,
	   COALESCE(redemption_cash,0) AS redemption_cash
FROM earning e
LEFT JOIN redemption r
ON r.redemption_user=e.earning_user)
SELECT 
	customer_id::int AS customer_id,
	User_classification,
	round(total_cash_earned,2) AS total_cash_earned,
	round(earned_cash_card,2) AS earned_cash_card,
	round(earned_cash_referral,2) AS earned_cash_referral,
	round(redemption_cash,2) AS redemption_cash,
	round(COALESCE(redemption_card,0),2) AS redemption_card,
	round(COALESCE(redemption_referral,0),2) AS redemption_referral,
	CASE WHEN COALESCE(redemption_cash,0)-(COALESCE(earned_cash_card,0)+COALESCE(earned_cash_referral,0))<0 THEN 0 
			  ELSE round(COALESCE(redemption_cash,0)-(COALESCE(earned_cash_card,0)+COALESCE(earned_cash_referral,0)),2) END AS over_redeemed 
FROM FINAL 
)
WITH NO SCHEMA binding;


