---Grover Card -- card level
	
    with a as (
		select event_timestamp,
		event_name,
		card_id, 
		user_id,
		external_user_id
		from
		where event_timestamp < '2021-07-19'
		union all
		select event_timestamp,event_name,
		JSON_EXTRACT_PATH_text(payload,'card_id') as card_id,
		JSON_EXTRACT_PATH_text(payload,'user_id')::int as user_id,
		JSON_EXTRACT_PATH_text(payload,'external_user_id') as external_user_id
		where event_timestamp >= '2021-07-19')
		, card_journey as (
		select distinct card_id,
		listagg(event_name,'->') within group (order by event_timestamp asc) as event_journey
		from 
		a
		group by 1
		)
		, status as 
		(select distinct card_id,
		useR_id,
		external_user_id,
		first_value(event_timestamp) over (partition by card_id order by event_timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_event_timestamp,
		LAST_VALUE(event_timestamp) over (partition by card_id order by event_timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_event_timestamp,
		first_value(event_name) over (partition by card_id order by event_timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_event_name,
		LAST_VALUE(event_name) over (partition by card_id order by event_timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_event_name
		from a )
	    select s.*,cj.event_journey  from status s 
	    left join card_journey cj on cj.card_id = s.card_id ;
        
        
	   with b as (
	   select user_id ,
	   max(case when accepted_tnc_at is not  null then event_timestamp end) within group (order by event_timestamp) as accepted_tnc_at,
	   min(event_timestamp) within group (order by event_timestamp)  as created_at,
	   min(status) within group (order by event_timestamp) as event_status_final,
	   listagg(distinct event_name,'->') within group (order by event_timestamp asc) as event_journey
	   group by 1   
	   )
	  ,a as (
	    select 
		    distinct user_id,
		    external_user_id,
	    first_value(event_name) over (partition by useR_id order by event_timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_event_name,
		LAST_VALUE(event_name) over (partition by useR_id order by event_timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_event_name
		select a.* ,
		b.created_at,
		b.accepted_tnc_at,
		b.event_status_final,
		b.event_journey
		from b 
		left join a on a.useR_id = b.user_id;
		
        
          
        ---Grover Card --user level
        
	     with a as (
		select event_timestamp,
		event_name,
		user_id,
		external_user_id
		from
		where event_timestamp < '2021-07-19'
		union all
		select event_timestamp,event_name,
		JSON_EXTRACT_PATH_text(payload,'user_id')::int as user_id,
		JSON_EXTRACT_PATH_text(payload,'external_user_id') as external_user_id
		where event_timestamp >= '2021-07-19')
		, card_journey as (
		select distinct user_id,
		listagg(event_name,'->') within group (order by event_timestamp asc) as event_journey
		from 
		a
		group by 1
		)
		, status as 
		(select distinct
		useR_id,
		external_user_id,
		first_value(event_timestamp) over (partition by useR_id order by event_timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_event_timestamp,
		LAST_VALUE(event_timestamp) over (partition by useR_id order by event_timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_event_timestamp,
		first_value(event_name) over (partition by useR_id order by event_timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_event_name,
		LAST_VALUE(event_name) over (partition by useR_id order by event_timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_event_name
		from a )
	    select s.*,cj.event_journey  from status s 
	    left join card_journey cj on cj.user_id = s.user_id ;
        
        
----- Grover card identification on user level --

		with a as (
		select *, 
		count(*) over (partition by user_id) as total_events,
		rank() over (partition by user_id order by event_timestamp asc) as rank_event 
		)
		, b as (
		select distinct user_id,
				LAST_VALUE(event_name)over (partition by useR_id order by event_timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as event_name_final,
		LAST_VALUE(status) over (partition by useR_id order by event_timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as event_status_final
		from 
		select 
		a.user_id,
		external_user_id,
		b.event_name_final,
		b.event_status_final,
		listagg(distinct status, '->' ) within group (order by event_timestamp asc) as status_journey,
		listagg(distinct event_name, '->' ) within group (order by event_timestamp asc) as event_journey,
		min(event_timestamp) within group (order by event_timestamp) as first_event_timestamp,
		max(event_timestamp) within group (order by event_timestamp) as last_event_timestamp
		from a 
		left join b on a.user_id = b.user_id
		group by 1,2,3,4;
        
  ----Grover card transactions on user level
			
			with card_level as (
			select  
				user_id,
				payload_id,
				card_id,
				card_scheme,
				currency,
				value,
				pos_entry_mode,
				transaction_date,
				event_name,
				merchant_town,
				merchant_country_code,
				transaction_type,
				event_timestamp,
				merchant_id,
				amount,
				Replace(SPLIT_PART(amount,',',1),'.','')::decimal(10,2) as amount_euro,
			TO_NUMBER(SPLIT_PART(SPLIT_PART(amount,',',2),'€',1),'99') as amount_cents,
			(amount_euro+(amount_cents/100))::decimal(10,2) as amount_transaction
			 from 
				 select user_id,
				 count(distinct card_id) as total_cards, 
				 sum(case when event_name = 'atm-withdrawal' then amount_transaction else 0 end) as amount_transaction_atm_withdrawal,
				 sum(case when event_name = 'card-transaction-refund' then amount_transaction else 0  end) as amount_transaction_card_transaction_refund,
				 sum(case when event_name = 'money-received' then amount_transaction else 0  end ) as amount_transaction_money_received,
				 min(case when event_name = 'money-received' then event_timestamp end) as first_timestamp_money_received, --Deposited Account
				 sum(case when event_name = 'payment-successful' then amount_transaction else 0  end) as amount_transaction_payment_successful,
				 min(case when event_name = 'payment-successful' then event_timestamp end) as first_timestamp_payment_successful, -- Active Card
				 sum(case when event_name = 'payment-successful'  and event_timestamp > CURRENT_DATE -31 then 1 else 0 end ) as is_active
                 from card_level
			group by 1;
			
            ----card transactions on card level
            
			select  
				user_id,
				payload_id,
				card_id,
				card_scheme,
				currency,
				value,
				pos_entry_mode,
				transaction_date,
				event_name,
				merchant_town,
				merchant_country_code,
				transaction_type,
				event_timestamp,
				merchant_id,
				amount,
				Replace(SPLIT_PART(amount,',',1),'.','')::decimal(10,2) as amount_euro,
			TO_NUMBER(SPLIT_PART(SPLIT_PART(amount,',',2),'€',1),'99') as amount_cents,
			(amount_euro+(amount_cents/100))::decimal(10,2) as amount_transaction
			 from 
 
 -----------Grover Card Requests           
			select distinct
			event_name,
			event_timestamp,
			nullif(regexp_replace(JSON_EXTRACT_PATH_text(payload,'user_id'),'([^0-9])',''),'')::int as user_id,
			JSON_EXTRACT_PATH_text(payload,'pre_contractual_info_sent_at')::date as card_request_date
            
             --------Grover Cash waiting user
select  nullif(regexp_replace(JSON_EXTRACT_PATH_text(payload,'user_id'),'([^0-9])',''),'')::int as user_id,
        DATE_TRUNC('second',JSON_EXTRACT_PATH_text(payload,'created_at')::timestamp)  as created_at,
		JSON_EXTRACT_PATH_text(payload,'state') as customer_state,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'waiting_list'),'name') as name,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'waiting_list'),'slug') as slug,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'waiting_list'),'description') as description
		from stg_kafka_events_full.stream_users_v2 where event_name='waiting_list_waiting' and slug='gc_de'; 


 ---card master user 
 
select  		
		distinct c.customer_id ,
		p.external_user_id as solaris_id,
		wl.created_at::date as waiting_list_date,
		p.created_at::date as user_created_date,
		gr.card_request_date::date as card_requested_date,
		p.accepted_tnc_at,
		p.event_status_final as card_person_status_final,
		p.event_journey as event_journey_card_person, 
		i.first_event_timestamp::date as card_created_date,
		i.last_event_timestamp::date as last_event_timestamp_identification,
		i.status_journey as status_journey_identification,
		i.event_journey as event_journey_identification,
		i.event_name_final as event_final_identification,
		i.event_status_final status_final_identification,
		u.first_event_name as card_event_first,
		u.last_event_name as card_event_final,
		u.first_event_timestamp::date as first_event_timestamp_card,
		u.last_event_timestamp::date as last_event_timestamp_card,
		u.event_journey as event_journey_user,
		tu.first_timestamp_money_received,
		tu.first_timestamp_payment_successful,
		tu.amount_transaction_atm_withdrawal,
		tu.amount_transaction_card_transaction_refund,
		tu.amount_transaction_money_received,
		tu.amount_transaction_payment_successful,
        case when tu.is_active > 0 then true else false end as is_active_card
           from ods_production.customer c 
		   where wl.created_at is not null or gr.card_request_date is not null or p.created_at is not null;
   
-----------Grover Card Redeem
--------Grover Cash redeem at user level
			select distinct
			event_name,
			event_timestamp,
			JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'amount'),'currency') as Currency,
			JSON_EXTRACT_PATH_text(payload,'user_id') as user_id,
			JSON_EXTRACT_PATH_text(payload,'date')::date as redeemed_date
			from stg_kafka_events_full.stream_internal_loyalty_service_credits silsc;

