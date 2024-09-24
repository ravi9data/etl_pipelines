drop table if exists ingram_events;
create temp table ingram_events as 
SELECT 
	first_value (i.serial_number) ignore nulls 
		over (partition by order_number order by source_timestamp asc
			 rows between unbounded preceding and unbounded following) as initial_serial_number ,
	last_value (i.serial_number) ignore nulls 
		over (partition by order_number order by source_timestamp asc
			 rows between unbounded preceding and unbounded following) as final_serial_number ,
	i.order_number, 
	--i.serial_number,
	i.asset_serial_number,
	i.package_serial_number ,
	coalesce ( first_value (i.new_serial_number) ignore nulls 
					over (partition by order_number order by source_timestamp desc
			 		rows between unbounded preceding and unbounded following),
			 	case when initial_serial_number <> final_serial_number 
		 			 then final_serial_number 
		 			 end 
			 	) as new_serial_number ,
	i.status_code,
	i.disposition_code,
	i.source_timestamp
FROM recommerce.ingram_micro_send_order_grading_status i
WHERE 1=1
qualify 
	case when disposition_code in ('LOCKED') 
		 then row_number() over	(PARTITION BY order_number, disposition_code, status_code ORDER BY source_timestamp desc)
		 else row_number() over	(PARTITION BY order_number, disposition_code, status_code ORDER BY source_timestamp asc)
	end = 1;


drop table if exists ods_operations.ingram_micro_orders;
create table ods_operations.ingram_micro_orders as
with
timestamps as (
	SELECT 
		order_number,
		initial_serial_number as serial_number,
		new_serial_number,
		--goods_in
		null::timestamp as pre_alert_at,
		max(CASE WHEN disposition_code ='GOODS_IN' AND status_code = 'SCANNED AT WAREHOUSE' THEN source_timestamp END) AS first_scan_at,
		max(CASE WHEN disposition_code ='GOODS_IN' AND status_code = 'RETURNED RETURN' THEN source_timestamp END) AS second_scan_at,
		max(CASE WHEN disposition_code ='RETURNED' AND status_code = 'RETURN RECEIVED' THEN source_timestamp END) AS return_confirmation_at,
		--clarification
		max(CASE WHEN disposition_code ='CLARIFICATION' AND status_code = 'CLARIFICATION START' THEN source_timestamp END) AS clarification_start_at,
		max(CASE WHEN disposition_code ='CLARIFICATION' AND status_code = 'CLARIFICATION END' THEN source_timestamp END) AS clarification_end_at,
		--grading
		max(CASE WHEN disposition_code ='GRADING' AND status_code = 'GRADING START' THEN source_timestamp END) AS grading_start_at,
		max(CASE WHEN disposition_code ='GRADING' AND status_code = 'GRADING FINISHED' THEN source_timestamp END) AS grading_finish_at,
		--channeling awaiting decision
		max(CASE WHEN disposition_code ='CHANNELING' AND status_code = 'AWAITING DECISION' THEN source_timestamp END) AS channeling_awaiting_decision_at,
		--check available,repair status_code 
		max(CASE WHEN disposition_code ='REPAIR' AND status_code = 'AVAILABLE, REPAIR' THEN source_timestamp END) AS available_for_repair_at,
		max(CASE WHEN disposition_code ='GOODS_OUT' AND status_code = 'SENT TO REPAIR PARTNER' THEN source_timestamp END) AS sent_to_repair_at,
		max(CASE WHEN disposition_code IN('GOODS_IN','RETURNED REPAIR') AND status_code in('RETURNED REPAIR','REPAIR RECEIVED') THEN source_timestamp END) AS returned_from_repair_partner,
		max(CASE WHEN disposition_code ='GRADING' AND status_code = 'LITE GRADING START' THEN source_timestamp END) AS lite_grading_start_at,
		max(CASE WHEN disposition_code ='GRADING' AND status_code = 'LITE GRADING FINISH' THEN source_timestamp END) AS lite_grading_finish_at,
		--check available,recommerce status_code 
		max(CASE WHEN disposition_code ='RECOMMERCE' AND status_code = 'AVAILABLE, RECOMMERCE' THEN source_timestamp END) AS available_for_recommerce_at,
		max(CASE WHEN disposition_code ='GOODS_OUT' AND status_code = 'SENT TO RECOMMERCE PARTNER' THEN source_timestamp END) AS sent_to_recommerce_at,
		--refurbishment
		max(CASE WHEN disposition_code ='REFURBISHMENT' AND status_code = 'AVAILABLE, REFURBISHMENT' THEN source_timestamp END) AS ref_start_at,
		max(CASE WHEN disposition_code ='REFURBISHMENT' AND status_code = 'REFURBISHMENT END' THEN source_timestamp END) AS ref_end_at,
		---stock
		max(CASE WHEN disposition_code ='GOODS_OUT' AND status_code = 'TRANSFER TO WH' THEN source_timestamp END) AS transfer_to_wh,
		--closure 
		max(CASE WHEN disposition_code ='LOCKED' THEN source_timestamp END) AS locked_at,
		max(CASE WHEN disposition_code ='CLOSED' THEN source_timestamp END) AS closed_at
	FROM ingram_events
	GROUP BY 1,2,3
)
SELECT 
	*
FROM timestamps t;
