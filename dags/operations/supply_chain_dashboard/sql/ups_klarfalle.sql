
--only use when there is a key including an uppercase
SET enable_case_sensitive_identifier TO true;

DROP TABLE IF EXISTS tmp_guil_jira_processed;
CREATE temp TABLE tmp_guil_jira_processed as
with
numbers as (
  	select ordinal from public.numbers where ordinal < 100
),
raw_jira as (
	select
		_airbyte_data."key"::text ticket_key,
		_airbyte_data."created"::timestamp created ,
		case when _airbyte_data."fields"."components"[0]."id"::int = 10694 then 'Inbound'
			 when _airbyte_data."fields"."components"[0]."id"::int = 10695 then 'Delivery Mgmt' end as department,
		coalesce (_airbyte_data."fields"."customfield_11221" ,
				  _airbyte_data."fields"."summary" )::text as po_number ,
		_airbyte_data."fields"."customfield_11215"::text as product_sku ,
		_airbyte_data."fields"."customfield_11229"::text as ean,
		_airbyte_data."fields"."status"."name"::text as status,
		coalesce (
			_airbyte_data."fields"."customfield_11217"[0]."value",
			_airbyte_data."fields"."customfield_11218"[0]."value")::text as case_status,
		coalesce (
			_airbyte_data."fields"."customfield_11214",
			_airbyte_data."fields"."customfield_11219") ::int as qty,
		_airbyte_data."changelog"."histories"[n.ordinal::int]."created"::timestamp as history_created,
		_airbyte_data."changelog"."histories"[n.ordinal::int]."items" as history_item,
		row_number() over (partition by _airbyte_data."key"::text
						   order by _airbyte_data."changelog"."histories"[n.ordinal::int]."created"::timestamp desc) rn
	from stg_external_apis."_airbyte_raw_guil_issues" j , numbers n
	where n.ordinal < get_array_length(_airbyte_data."changelog"."histories")
	and _airbyte_data."fields"."components"[0]."id"::int in (10694, 10695)
),
parse_history as (
	select
		ticket_key ,
		history_created,
		case when history_item[n.ordinal::int]."field"::text = 'status'
			 then upper(history_item[n.ordinal::int]."toString"::text)
			 end _pre_new_status,
		case when count(case when _pre_new_status is not null then 1 end)
					over (partition by ticket_key) = 0
			 then upper(status)
			 else _pre_new_status end as new_status
	from raw_jira r , numbers n
	where n.ordinal < get_array_length(history_item)
),
calc_timestamps as (
	select
		ticket_key ,
		max(case when new_status in ('TO DO', 'TO DO UPS', 'TO DO GROVER') then history_created end) to_do_at,
		max(case when new_status in ('AWAITING RESPONSE UPS', 'AWAITING RESPONSE GROVER') then history_created end) waiting_response_at,
		max(case when new_status in ('IN PROGRESS UPS', 'IN PROGRESS GROVER') then history_created end) in_progress_at,
		--
		--
		max(case when new_status in ('TO DO UPS', 'AWAITING RESPONSE UPS', 'IN PROGRESS UPS') then history_created end) pending_ups,
		max(case when new_status in ('TO DO GROVER', 'AWAITING RESPONSE GROVER', 'IN PROGRESS GROVER') then history_created end) pending_grover,
		--
		--
		max(case when new_status in ('DONE', 'SOLVED GROVER') then history_created end) _pre_solved_at,
		case when coalesce (to_do_at,waiting_response_at,in_progress_at) is not null
			  and greatest (to_do_at,waiting_response_at,in_progress_at) > _pre_solved_at
		 then null
		 else _pre_solved_at end solved_at
	from parse_history
	where new_status is not null
	group by 1)
select
	r.ticket_key,
	r.created,
	r.department ,
	r.po_number ,
	r.ean,
	case when r.product_sku like 'GR%'
		 then r.product_sku
	end as product_sku ,
	r.case_status ,
	r.qty,
	c.to_do_at,
	c.waiting_response_at,
	c.in_progress_at,
	c.pending_grover,
	c.pending_ups ,
	c.solved_at
from raw_jira r
left join calc_timestamps c
on r.ticket_key = c.ticket_key
--take the last history received
--unfortutanely we receive a new row
--when one of the main fields is updated
where r.rn = 1;

SET enable_case_sensitive_identifier TO false;



DROP TABLE IF EXISTS stg_external_apis.guil_jira_processed;
CREATE TABLE stg_external_apis.guil_jira_processed as
select
	ticket_key,
	created,
	department,
	po_number,
	ean,
	product_sku,
	case_status,
	qty,
	case when department = 'Inbound'
		 then coalesce (
					(select pr.purchase_price
					from ods_production.purchase_request_item pr
					left join ods_production.variant v
					on pr.variant_sku = v.variant_sku
					where po_number = pr.purchase_order_number
					  and ean = v.ean
					order by pr.purchased_date desc limit 1)
					,
					(select avg(t.initial_price::float) from master.asset t
						where
							(product_sku = t.variant_sku) or
							(split_part(product_sku, 'V', 1) = t.product_sku) or
							(ean::text = t.ean)
						and t.created_at > current_date - 360 )
					,
					(select sum(total_price) / sum(purchase_quantity)
						from ods_production.purchase_request_item pr
						where (po_number = pr.purchase_order_number) or
							  (product_sku = pr.variant_sku)
							  )
					) end as purchase_price,
	to_do_at,
	waiting_response_at,
	in_progress_at,
	pending_ups ,
	pending_grover ,
	solved_at
from tmp_guil_jira_processed;
