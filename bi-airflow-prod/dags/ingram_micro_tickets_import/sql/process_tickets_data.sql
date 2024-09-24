
delete from stg_external_apis.ingram_tickets_data
where snapshot_date = current_date;

insert into stg_external_apis.ingram_tickets_data
SELECT
	ticket_id,
	"timestamp",
	work_order_id,
	carrier,
	tracking_number,
	email_ingram,
	customer_address,
	image_address,
	clarification_reason,
	grover_team_responsible,
	responsible_team_email,
	serial_number_asset,
	serial_number_box,
	comments,
	customer_belongings_additional_details,
	gdpr_flag,
	device_turn_on_flag,
	sku,
	gr_open_flag,
	gr_processing_flag,
	gr_closed_cancelled_flag,
	grover_status_open_ts,
	grover_status_processing_ts,
	grover_status_closed_ts,
	ticket_status_grover,
	solution,
	json_created,
	current_date AS snapshot_date
FROM
	stg_external_apis_dl.ingram_tickets_data;
