---Grover Card -- card level
with a as (
		select event_timestamp::timestamp ,
		event_name,
		card_id, 
		user_id as customer_id,
		external_user_id
		from
		where event_timestamp < '2021-07-19'-- AND user_id='1159697'
		union all
		select event_timestamp::timestamp,
		event_name,
		JSON_EXTRACT_PATH_text(payload,'card_id') as card_id,
		JSON_EXTRACT_PATH_text(payload,'user_id')::int as customer_id,
		JSON_EXTRACT_PATH_text(payload,'external_user_id') as external_user_id
		where event_timestamp >= '2021-07-19' and event_name not in ('device-bound','pin_set')-- AND user_id='1159697' 
		)
		, card_journey as (
		select distinct card_id,
		listagg(event_name,'->') within group (order by event_timestamp asc) as event_journey
		from a
		group by 1
		)
		, stop as (
		select *,
		row_number () over (partition by customer_id, card_id order by event_timestamp asc) rn,
		count(1) over (partition by customer_id, card_id order by event_timestamp desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) cnt
		,row_number () over (partition by customer_id, (case when event_name='activated' then 1 else 0 end) order by event_timestamp asc) as col_1
		from a )
		, status as (
		select distinct
			customer_id ,
			card_id ,
			external_user_id ,
			min(case when rn = 1 then event_name end) as first_event_name,
			min(case when rn = cnt then event_name end) as last_event_name,
			min(case when rn = 1 then event_timestamp end) as first_event_timestamp,
			min(case when rn = cnt then event_timestamp end) as last_event_timestamp,
			min(case when col_1 = 1 and event_name ='activated' then event_timestamp end) as first_activated_timestamp
		from stop group by 1,2,3
		)
	  select s.*,cj.event_journey,
    ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY first_event_timestamp) as Latest_card 
    from status s 
	  left join card_journey cj on cj.card_id = s.card_id;


       -----------Grover Card Requests           
			select distinct
			event_name,
			event_timestamp::timestamp,
			nullif(regexp_replace(JSON_EXTRACT_PATH_text(payload,'user_id'),'([^0-9])',''),'')::int as customer_id,
			JSON_EXTRACT_PATH_text(payload,'pre_contractual_info_sent_at')::date as card_request_date
            
