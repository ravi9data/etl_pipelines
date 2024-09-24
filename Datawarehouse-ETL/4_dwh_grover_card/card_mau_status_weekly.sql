WITH dates AS (
	SELECT 
		datum ,
		day_is_first_of_month
	FROM public.dim_dates  
	WHERE TRUE 
	AND week_day_number =1
	AND datum BETWEEN '2021-03-01' AND current_date)
,customers as (
	SELECT 
		DISTINCT customer_id,
		first_event_timestamp AS card_created_date
	WHERE TRUE 
	AND latest_card=1
	)
, customer_data AS (
	SELECT 
		datum AS c_date, 
		day_is_first_of_month,
		customer_id,
		CASE WHEN c.card_created_date IS NOT NULL AND date_trunc('week',card_created_date)::date<=datum::date THEN 1 ELSE 0 END AS card_created
	FROM dates dd 
	CROSS JOIN customers c 
	ORDER BY 2,1
	)
,deposits as (
	SELECT 
		DISTINCT customer_id,
		min(transaction_date) AS deposit_date 
	WHERE event_name='money-received'
	GROUP BY 1
	)	
,deposit_data AS (
	SELECT 
		datum AS d_date, 
		customer_id,
		CASE WHEN d.deposit_date IS NOT NULL AND date_trunc('week',deposit_date)::date<=datum::date THEN 1 ELSE 0 END AS money_deposited
	FROM dates dd 
	CROSS JOIN deposits d
	ORDER BY 2,1
	)
, closed_accounts_date AS (
	SELECT 
		customer_id,
	   legal_closure_date
,closed_accounts AS (
	SELECT 
		datum AS ca_date, 
		customer_id,
		CASE WHEN legal_closure_date IS NOT NULL AND date_trunc('week',legal_closure_date)::date<=datum::date THEN 1 ELSE 0 END AS account_closed
	FROM dates dd 
	CROSS JOIN closed_accounts_date cad
	ORDER BY 2,1)
,payments as (
	SELECT 
		DISTINCT customer_id,
		date_trunc('week',transaction_date) AS t_date 
	WHERE event_name='payment-successful')	
,base AS (
	SELECT 
		c_date,
		c.day_is_first_of_month,
		c.customer_id,
		c.card_created,
		COALESCE(d.money_deposited,0) AS money_deposited,
		COALESCE(ca.account_closed,0) AS account_closed,
	   CASE WHEN p.customer_id IS NOT NULL  THEN 1 ELSE 0 END AS has_transactions,
	   COALESCE(LAG(has_transactions)OVER (PARTITION BY c.customer_id ORDER BY c_Date),0) AS prev_transactions,
	   CASE WHEN has_transactions+prev_transactions=1 THEN 1 ELSE 0 END AS sum_transactions
    FROM customer_data c
	left JOIN deposit_data d 
	ON c.c_date=d.d_Date
		AND c.customer_id=d.customer_id
	LEFT JOIN payments p
		ON c.c_date=p.t_Date
		AND c.customer_id=p.customer_id
	LEFT JOIN closed_accounts ca 
		ON c.customer_id=ca.customer_id
		AND c.c_date=ca.ca_Date
	WHERE TRUE 
	AND card_created=1
	ORDER BY 2,1
	)
,sum_ AS (
	SELECT 
		*,
 		sum(sum_transactions) OVER (PARTITION BY customer_id ORDER BY c_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_idx
 	FROM base
)
,final_pre AS (
	SELECT *,
		ROW_NUMBER() OVER (Partition BY customer_id,sum_idx ORDER BY c_date) idx
	FROM sum_	 )
,FINAL AS (
SELECT 
c_date AS week_,
customer_id,
card_created,
money_deposited,
account_closed,
has_transactions,
CASE 
	WHEN account_closed=1 THEN 'Account Closed' 
	ELSE 
		CASE
			WHEN money_deposited=0 AND has_transactions=0 THEN 'Registered'
			WHEN money_deposited=1 AND has_transactions=0 AND sum_idx=0 THEN 'Financial Activated'
			WHEN money_deposited IN (1,0) AND has_transactions=1 AND idx=1 THEN 'MAU +1'
			WHEN money_deposited IN (1,0) AND has_transactions=1 AND idx=2 THEN 'MAU +2'
			WHEN money_deposited IN (1,0) AND has_transactions=1 AND idx BETWEEN 3 AND 5 THEN 'MAU +3'
			WHEN money_deposited IN (1,0) AND has_transactions=1 AND idx BETWEEN 6 AND 11 THEN 'MAU +6'
			WHEN money_deposited IN (1,0) AND has_transactions=1 AND idx BETWEEN 12 AND 17 THEN 'MAU +12'
			WHEN money_deposited IN (1,0) AND has_transactions=1 AND idx>=18 THEN 'MAU +18'
			WHEN money_deposited IN (1,0) AND has_transactions=0 AND sum_idx>0 AND idx=1 THEN 'Inactive -1'
			WHEN money_deposited IN (1,0) AND has_transactions=0 AND sum_idx>0 AND idx=2 THEN 'Inactive -2'
			WHEN money_deposited IN (1,0) AND has_transactions=0 AND sum_idx>0 AND idx BETWEEN 3 AND 5 THEN 'Inactive -3'
			WHEN money_deposited IN (1,0) AND has_transactions=0 AND sum_idx>0 AND idx BETWEEN 6 AND 11 THEN 'Inactive -6'
			WHEN money_deposited IN (1,0) AND has_transactions=0 AND sum_idx>0 AND idx >=12 THEN 'Inactive -12'
		END 
	END 
AS MAU_status,
LAG(mau_status)OVER(PARTITION BY customer_id ORDER BY week_) AS Last_Mau_status
FROM FINAL_pre )
SELECT *,
CASE WHEN MAU_status ='Registered' THEN 'Registered' 
	  WHEN MAU_status ='Financial Activated' THEN 'Financial Activated'
	  WHEN (Last_mau_status IN ('Financial Activated','Registered') OR Last_mau_status IS NULL ) AND MAU_status='MAU +1' THEN 'New'
	  WHEN Last_mau_status ILIKE '%MAU%' AND MAU_status ILIKE '%MAU%'  THEN 'Retained'
	  WHEN Last_mau_status ILIKE '%MAU%' AND MAU_status ILIKE '%Inactive%'  THEN 'Dormant'
	  WHEN Last_mau_status ILIKE '%Inactive%' AND MAU_status ILIKE '%Inactive%'  THEN 'Dormant'
	  WHEN Last_mau_status ILIKE '%Inactive%' AND MAU_status ILIKE '%MAU%'  THEN 'Resurrected'
	  WHEN MAU_status ='Account Closed' THEN 'Account Closed' 
END AS customer_status
FROM FINAL
WITH NO SCHEMA BINDING;


