DROP TABLE IF EXISTS dm_operations.reverse_lead_times;
CREATE TABLE dm_operations.reverse_lead_times AS 
WITH reverse_lead_times AS (
SELECT
	i.order_number,
	i.serial_number,
	(
	SELECT
		return_delivery_date
	FROM
		ods_operations.allocation_shipment a
	WHERE
		a.serial_number = i.serial_number
		AND a.allocated_at<COALESCE(i.first_scan_at,
		i.second_scan_at)
		AND a.return_shipment_label_created_at IS NOT NULL
	ORDER BY
		a.allocated_at DESC
	LIMIT 1) AS time_to_unpack_start,
	COALESCE(COALESCE(i.return_confirmation_at,
	i.clarification_end_at),
	i.grading_start_at) AS time_to_unpack_end,
	COALESCE(COALESCE(COALESCE(i.available_for_repair_at,
	i.available_for_recommerce_at),
	i.channeling_awaiting_decision_at),
	i.locked_at) AS time_to_grade_end,
	COALESCE(COALESCE(i.returned_from_repair_partner,
	i.lite_grading_start_at),
	i.lite_grading_finish_at) AS time_to_repair_end
FROM
	ods_operations.ingram_micro_orders i
)

SELECT
	order_number,
	serial_number,
	datediff('day',
	time_to_unpack_start,
	time_to_unpack_end) AS time_to_unpack,
	datediff('day',
	time_to_unpack_end,
	time_to_grade_end) AS time_to_grade,
	datediff('day',
	time_to_grade_end,
	time_to_refurbish_end) AS time_to_refurbish,
	datediff('day',
	time_to_grade_end,
	time_to_repair_end) AS time_to_repair
FROM
	reverse_lead_times;