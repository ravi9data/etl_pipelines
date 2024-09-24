truncate table stg_external_apis_dl.sc_eu_inbound_schedule;

insert into stg_external_apis_dl.sc_eu_inbound_schedule
SELECT
	"year"::bigint,
	"month"::bigint as month_num,
    cw::bigint as week_num,
    "date" as schedule_date,
    supplier as supplier,
    "we/pr" as pr_number,
    qty::bigint as quantity,
    null::text as subcategory_name,
    "packing units" as packing_units,
    "carrier/tracking" as carrier,
    destination as warehouse,
    prio as priority,
    "pod link"::text as pod_link,
    case when "pod requested" = 'FALSE' then false else true end  as pod_requested,
    case when "booking request" = 'FALSE' then false else true end as booking_request,
    "klärfall?" is_klarfalle,
    "falsch gesc. sn?" is_false_sn,
    "ib false booking?"	is_false_booking,
    "pr status" as pr_status,
    "pr total qty"::bigint as pr_total_quantity,
    "booked assets"::bigint	as booked_assets,
    "completed in mt/jira?"	as is_completed_mt_jira,
    "pending allocations >5?" as is_pending_allocation,
    null::text as payment_date,
    "split delivery?" as is_split_delivery,
    case when "booking requested - ib?" = 'FALSE' then false else true end as is_booking_requested
from staging.grover_eu_inbound_forecast_master_history;


delete from stg_external_apis.sc_eu_inbound_schedule
where reporting_date = current_date;

insert into stg_external_apis.sc_eu_inbound_schedule
select
	current_date as reporting_date,
    week_num ,
    warehouse,
    sum(quantity) as weekly_quantity
from stg_external_apis_dl.sc_eu_inbound_schedule
where week_num = date_part(w, current_date)
and "year" = date_part(y, current_date)
and warehouse is not null
group by 1, 2, 3;
