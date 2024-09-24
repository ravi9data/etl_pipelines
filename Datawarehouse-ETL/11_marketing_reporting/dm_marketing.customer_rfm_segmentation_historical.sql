DROP TABLE IF EXISTS tmp_rfm_segmentation;
CREATE TEMP TABLE tmp_rfm_segmentation
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
WITH customer_data AS (					
	SELECT DISTINCT 					
		c.customer_id,					
		(c.committed_subscription_value::float)/(c.subscriptions::float) as avg_csv,									
		c.subscriptions as subs								
	from master.customer c					
)	
-- finding the recent paid order date
, order_ AS (					
	SELECT DISTINCT  
		o.customer_id,	
        FIRST_VALUE(o.created_date) IGNORE NULLS OVER
                (PARTITION BY o.customer_id ORDER BY o.created_date DESC 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_order_created_date,
        datediff('month', last_order_created_date::date, current_date) AS months_since_last_order	
	FROM master.order o									
	WHERE status = 'PAID'	
)									
	SELECT 
		c.customer_id, 				
		-- Recency					
		case					
			when months_since_last_order <= 3 then '5'				
			when (months_since_last_order > 3 and months_since_last_order <= 6) then '4'				
			when (months_since_last_order > 6 and months_since_last_order <= 12) then '3'				
			when (months_since_last_order > 12 and months_since_last_order <= 24) then '2'				
			when (months_since_last_order > 24 or months_since_last_order is null ) then '1'				
		end as recency,					
		-- Frequency					
		case					
			when subs >= 3 then '5'				
			when subs = 3 then '4'				
			when subs = 2 then '3'				
			when subs = 1 then '2'				
			when subs = 0 or subs is null then '1'				
		end as frequency,					
		-- Monetary					
		case					
			when avg_csv >= 600 then '5'				
			when avg_csv > 400 then '4'				
			when avg_csv > 250 then '3'				
			when avg_csv > 100 then '2'				
			when (avg_csv <= 100 or avg_csv is null) then '1'				
		end as monetary	,
		-- Classification
		CASE 
			WHEN recency >= 4 AND frequency >= 4 AND monetary >= 4 THEN 'Loyal'
			WHEN recency <= 3 AND frequency >= 4 AND monetary >= 3 THEN 'Low Recency Loyal'
			WHEN recency >= 4 AND frequency >= 4 AND monetary <= 3 THEN 'Low Monetary Loyal'
			WHEN (frequency BETWEEN 2 AND 4) AND monetary = 5 THEN 'Big Spender'
			WHEN (frequency BETWEEN 2 AND 4) AND monetary = 1 THEN 'Small Spender'
			WHEN (recency BETWEEN 3 AND 4) AND (frequency BETWEEN 2 AND 3) AND (monetary BETWEEN 2 AND 4) THEN 'Middleman'
			WHEN recency <= 2 AND (frequency BETWEEN 2 AND 3) AND (monetary BETWEEN 2 AND 4) THEN 'Low Recency Middleman'
			WHEN recency = 5 AND (frequency BETWEEN 2 AND 4) AND (monetary BETWEEN 2 AND 4) THEN 'Recent Renter'
			WHEN frequency = 1 THEN 'Prospect'
			WHEN recency <= 3 AND frequency >= 4 AND monetary <= 2 THEN 'Cheap Frequent Renter'
			ELSE 'Other'
		END AS rfm_segmentation,
		current_date::date AS date
	FROM customer_data c					
	LEFT JOIN order_ o 
		ON o.customer_id = c.customer_id 	
;

BEGIN TRANSACTION;

DELETE FROM dm_marketing.customer_rfm_segmentation_historical
WHERE customer_rfm_segmentation_historical.date = current_date::date 
		OR (customer_rfm_segmentation_historical.date < DATEADD('month', -1,current_date) --keeping the past 30 days AND BEFORE it ONLY the 1st DAY OF MONTH AND 1st DAY OF week
			AND (date_trunc('month', customer_rfm_segmentation_historical.date) <> customer_rfm_segmentation_historical.date 
				AND date_trunc('week', customer_rfm_segmentation_historical.date) <> customer_rfm_segmentation_historical.date 
				AND customer_rfm_segmentation_historical.date <> '2022-12-31'));

INSERT INTO dm_marketing.customer_rfm_segmentation_historical
SELECT * 
FROM tmp_rfm_segmentation;
 
END TRANSACTION;

GRANT ALL ON ALL TABLES IN SCHEMA MARKETING TO GROUP BI;
GRANT select ON ALL TABLES IN SCHEMA dm_marketing TO redash_growth;

GRANT SELECT ON dm_marketing.customer_rfm_segmentation_historical TO tableau;
		
