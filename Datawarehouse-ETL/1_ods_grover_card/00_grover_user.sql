       
        
      with b as (
  select * from (
		   select user_id as customer_id ,
		   external_user_id ,
		   max(case when accepted_tnc_at is not  null then event_timestamp::timestamp end) within group (order by event_timestamp::timestamp) as accepted_tnc_at,
		   min(event_timestamp::timestamp) within group (order by event_timestamp::timestamp)::timestamp  as created_at,
		   min(case when event_name = 'screening' then event_timestamp::timestamp end)  as last_event_timestamp,
	   min(status) within group (order by event_timestamp::timestamp) as event_status_final,
	   listagg(distinct event_name,'->') within group (order by event_timestamp::timestamp asc) as event_journey,
		   row_number () over (partition by user_id order by created_at desc) as occurance 
		   group by 1,2
		   ) a where occurance=1
   )
  ,a as (
    select 
	    distinct user_id as customer_id,
    first_value(event_name) over (partition by user_id order by event_timestamp::timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_event_name,
	LAST_VALUE(event_name) over (partition by user_id order by event_timestamp::timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_event_name	   
	select a.customer_id,
	b.external_user_id,
	a.first_event_name,
	a.last_event_name,
	b.created_at::timestamp,
	b.last_event_timestamp::timestamp,
	b.accepted_tnc_at,
	b.event_status_final,
	b.event_journey
	from b 
	left join a on a.customer_id = b.customer_id;
	
        ---Grover Card --user level
        
	     with a as (
		select event_timestamp::timestamp,
		event_name,
		user_id as customer_id,
		external_user_id
		from
		where event_timestamp::timestamp < '2021-07-19'
		union all
		select event_timestamp::timestamp,event_name,
		JSON_EXTRACT_PATH_text(payload,'user_id')::int as customer_id,
		JSON_EXTRACT_PATH_text(payload,'external_user_id') as external_user_id
		where event_timestamp::timestamp >= '2021-07-19')
		, card_journey as (
		select distinct customer_id,
		listagg(event_name,'->') within group (order by event_timestamp::timestamp asc) as event_journey
		from 
		a
		group by 1
		)
		, status as 
		(select distinct
		customer_id,
		external_user_id,
		first_value(event_timestamp::timestamp) over (partition by customer_id order by event_timestamp::timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_event_timestamp,
		LAST_VALUE(event_timestamp::timestamp) over (partition by customer_id order by event_timestamp::timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_event_timestamp,
		first_value(event_name) over (partition by customer_id order by event_timestamp::timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_event_name,
		LAST_VALUE(event_name) over (partition by customer_id order by event_timestamp::timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_event_name
		from a )
	    select s.*,cj.event_journey  from status s 
	    left join card_journey cj on cj.customer_id = s.customer_id ;
        

--------Grover Cash waiting user
select  nullif(regexp_replace(JSON_EXTRACT_PATH_text(payload,'user_id'),'([^0-9])',''),'')::int as customer_id,
        DATE_TRUNC('second',JSON_EXTRACT_PATH_text(payload,'created_at')::timestamp)  as created_at,
		JSON_EXTRACT_PATH_text(payload,'state') as customer_state,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'waiting_list'),'name') as name,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'waiting_list'),'slug') as slug,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'waiting_list'),'description') as description
		from stg_kafka_events_full.stream_users_v2 where event_name='waiting_list_waiting' and slug='gc_de';

