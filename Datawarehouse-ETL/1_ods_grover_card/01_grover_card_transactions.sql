	
            ----card transactions on card level
            
select 
    DISTINCT 
        user_id::int as customer_id,
        trace_id,
        id AS payload_id,
        cards_card_id AS card_id,
        card_scheme,
        orginal_amount_currency AS currency,
        pos_entry_mode,
        event_name,
        merchant_town,
        merchant_country_code,
        merchant_id,
        merchant_name,
        merchant_category_code,
        transaction_type,
        wallet_type,
        CASE WHEN event_name IN ('money-received','money-sent') THEN date::timestamp
        ELSE 
            CASE 
                WHEN transaction_time IS NULL OR transaction_time =' ' THEN transaction_date::timestamp 
            ELSE transaction_time::timestamp
            END END AS event_timestamp,
        event_timestamp::date AS transaction_date,
        NULL AS reason,
        Replace(SPLIT_PART(amount,',',1),'.','')::decimal(10,2)+
        (TO_NUMBER(SPLIT_PART(SPLIT_PART(amount,',',2),'€',1),'99')/100)::decimal(10,2) as amount_transaction
        --consumed_at::timestamp AS recorded_at 
        from 
UNION ALL ----Failed Payments
SELECT 
    * 
WHERE event_name ='payment-failed';

 
     
  ----Grover card transactions on user level
			
			with card_level as (
			select 
				DISTINCT 
					user_id::int as customer_id,
					trace_id,
					id AS payload_id,
					cards_card_id AS card_id,
					card_scheme,
					orginal_amount_currency AS currency,
					pos_entry_mode,
					event_name,
					merchant_town,
					merchant_country_code,
					merchant_id,
					merchant_name,
					merchant_category_code,
					transaction_type,
					wallet_type,
					CASE WHEN event_name IN ('money-received','money-sent') THEN date::timestamp
					ELSE 
						CASE 
							WHEN transaction_time IS NULL OR transaction_time =' ' THEN transaction_date::timestamp 
						ELSE transaction_time::timestamp
						END END AS event_timestamp,
					event_timestamp::date AS transaction_date,
					NULL AS reason,
					Replace(SPLIT_PART(amount,',',1),'.','')::decimal(10,2)+
					(TO_NUMBER(SPLIT_PART(SPLIT_PART(amount,',',2),'€',1),'99')/100)::decimal(10,2) as amount_transaction
					--consumed_at::timestamp AS recorded_at 
					from 
			UNION ALL ----Failed Payments
			SELECT 
				* 
			WHERE event_name ='payment-failed')
							select customer_id,
							count(distinct card_id) as total_cards, 
							min(case when event_name = 'atm-withdrawal' then event_timestamp::timestamp end) as first_timestamp_atm_withdrawal,
							sum(case when event_name = 'atm-withdrawal' then amount_transaction else 0 end) as amount_transaction_atm_withdrawal,
							min(case when event_name = 'card-transaction-refund' then event_timestamp::timestamp  end) as first_timestamp_transaction_refund,
							sum(case when event_name = 'card-transaction-refund' then amount_transaction else 0  end) as amount_transaction_card_transaction_refund,
							sum(case when event_name = 'money-received' then amount_transaction else 0  end ) as amount_transaction_money_received,
							min(case when event_name = 'money-received' then event_timestamp::timestamp end) as first_timestamp_money_received, --Deposited Account
							sum(case when event_name = 'payment-successful' then amount_transaction else 0  end) as amount_transaction_payment_successful,
							min(case when event_name = 'payment-successful' then event_timestamp::timestamp end) as first_timestamp_payment_successful, -- Active Card
							sum(case when event_name = 'payment-failed' then amount_transaction else 0  end) as amount_transaction_payment_failed,
							min(case when event_name = 'payment-failed' then event_timestamp::timestamp end) as first_timestamp_payment_failed, -- Active Card
							sum(case when event_name = 'payment-successful'  and event_timestamp::timestamp > CURRENT_DATE-31 then 1 else 0 end ) as is_active
							from card_level
						group by 1;
		
		--Grover Fees
		 SELECT 
			JSON_EXTRACT_PATH_TEXT(payload,'user_id')::int AS customer_id,
			event_name,
			JSON_EXTRACT_PATH_TEXT(payload,'currency')AS currency,
			JSON_EXTRACT_PATH_TEXT(payload,'amount')/100::int AS Fee,
			CASE WHEN JSON_EXTRACT_PATH_TEXT(payload,'reason')='atm' THEN 'atm-withdrawal' ELSE JSON_EXTRACT_PATH_TEXT(payload,'reason') end AS reason,
			CASE 
				WHEN event_name='executed' THEN NULLIF(JSON_EXTRACT_PATH_TEXT(payload,'chargedAt'),'')::timestamp 
			ELSE 
				NULLIF(JSON_EXTRACT_PATH_TEXT(payload,'collection_date'),'')::timestamp
			END AS collection_date,
			NULLIF(JSON_EXTRACT_PATH_TEXT(payload,'reference_date'),'')::timestamp AS reference_date,
			NULLIF(JSON_EXTRACT_PATH_TEXT(payload,'referenceId'),'') AS reference_id,
			NULLIF(JSON_EXTRACT_PATH_TEXT(payload,'reconciliationId'),'') AS reconciliation_id

