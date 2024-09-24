   
----- Grover card identification on user level --

		with a as (
		select *, 
		count(*) over (partition by user_id) as total_events,
		rank() over (partition by user_id order by event_timestamp::timestamp asc) as rank_event,
		row_number () over (partition by user_id order by event_timestamp::timestamp desc) as occurance 
		)
		, b as (
		select distinct user_id AS customer_id,
				LAST_VALUE(event_name)over (partition by useR_id order by event_timestamp::timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as event_name_final,
		LAST_VALUE(status) over (partition by useR_id order by event_timestamp::timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as event_status_final
		from 
		select 
		customer_id,
		external_user_id,
		b.event_name_final,
		b.event_status_final,
		listagg(distinct status, '->' ) within group (order by event_timestamp::timestamp asc) as status_journey,
		listagg(distinct event_name, '->' ) within group (order by event_timestamp::timestamp asc) as event_journey,
		min(event_timestamp::timestamp) within group (order by event_timestamp::timestamp) as first_event_timestamp,
		max(event_timestamp::timestamp) within group (order by event_timestamp::timestamp) as last_event_timestamp
		from a 
		left join b on a.user_id = b.customer_id
		where occurance=1
		group by 1,2,3,4;

