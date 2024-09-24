with base as (
Select  date_trunc('day',event_timestamp)as date_, 
		 COALESCE(sum(CASE WHEN event_name = 'money-received' THEN amount_transaction END ),0) as Deposited_amount,
		 COALESCE(sum(CASE WHEN event_name = 'payment-successful' THEN amount_transaction END ),0)as transaction_amount,
		 COALESCE(sum(CASE WHEN event_name = 'card-transaction-refund' THEN amount_transaction END ),0)  as refund_amount,
		 COALESCE(sum(CASE WHEN event_name = 'money-sent' THEN amount_transaction END ),0)  as Transferred_amount,
		 (Deposited_amount+refund_amount-transaction_amount-Transferred_amount) AS day_end_balance,
		 COALESCE(LAG(day_end_balance) OVER (ORDER BY date_),0) prev_day_end_balance,
		 ROW_NUMBER ()OVER(ORDER BY date_) idx
GROUP BY 1)
SELECT 
*
FROM base
ORDER BY 1
WITH NO SCHEMA binding;

with recursive amt (date_,
	Deposited_amount ,
	transaction_amount,
	refund_amount,
	Transferred_amount,
	day_end_balance,
	balance,
	idx
)as
(SELECT 
	date_,
	Deposited_amount ,
	transaction_amount,
	refund_amount,
	Transferred_amount,
	day_end_balance,
	day_end_balance::numeric(30,3) AS balance,
	idx
	UNION ALL 
SELECT 
	e.date_::timestamp,
	e.Deposited_amount::numeric(30,3) ,
	e.transaction_amount::numeric(30,3),
	e.refund_amount::numeric(30,3),
	e.Transferred_amount::numeric(30,3),
	e.day_end_balance::numeric(30,3),
	(j.balance+(e.Deposited_amount+e.refund_amount-e.transaction_amount-e.Transferred_amount))::numeric(30,3) AS balance,
	e.idx
	FROM amt j 
		 ON j.idx=e.idx-1
		 )
		 SELECT date_,
		 Deposited_amount,
		 transaction_amount,
		 refund_amount,
		 Transferred_amount,
		 balance AS EOD_balance,
		 avg(balance) OVER (ORDER BY date_ ROWS BETWEEN 6 PRECEDING AND CURRENT ROW ) AS seven_day_rolling_avg,
		 avg(balance) OVER (ORDER BY date_ ROWS BETWEEN 30 PRECEDING AND CURRENT ROW ) AS thirty_day_rolling_avg
		 FROM amt);

