with card as(
select
	distinct customer_id,
	first_value(first_event_timestamp) over (partition by customer_id
order by
	first_event_timestamp asc
                    rows between unbounded preceding and unbounded following) as first_card_created_date,
	first_value(first_event_name) over (partition by customer_id
order by
	first_event_timestamp desc
                    rows between unbounded preceding and unbounded following) as Card_first_event,
	first_value(last_event_name) over (partition by customer_id
order by
	first_event_timestamp desc
                    rows between unbounded preceding and unbounded following) as Card_last_event,
	first_value(first_activated_timestamp) over (partition by customer_id
order by
	first_activated_timestamp asc
                    rows between unbounded preceding and unbounded following) as first_activated_timestamp,
	count(customer_id) over (partition by customer_id ) as total_card_count
from
where
	first_event_name = 'created' )
,
card_requested as (
select
	customer_id,
	min(event_timestamp) as card_requested_date
from
where
	customer_id is not null
group by
	1)
,total_subscriptions as (
select
	gc2.customer_id,
	first_subscription_start_date,
	first_card_created_date as card_created_date,
	s.subscriptions_per_customer,
	Avg(rental_period) as avg_card_rental_period,
	max(case when cancellation_date<first_card_created_date then cancellation_date end ) as max_cancellation_date_before_card,
	min(case when start_date>first_card_created_date then start_date end ) as first_subscription_start_date_after_card,
	count(distinct case when s.start_date<first_card_created_date then s.subscription_id end ) as total_subscription_before_card,
	count(distinct case when s.start_date>first_card_created_date then s.subscription_id end) as total_card_subscriptions,
	count(distinct case when status = 'ACTIVE' then s.subscription_id end) as active_subscriptions,
	sum(case when status = 'ACTIVE' then s.subscription_value end) as active_subscription_value,
	count(case when s.start_date>first_card_created_date and status = 'ACTIVE' then s.subscription_id end) as active_card_subscriptions,
	sum(case when s.start_date<first_card_created_date then s.subscription_value end ) as total_subscription_value_before_card,
	sum(case when s.start_date>first_card_created_date then s.subscription_value end) as total_card_subscription_value,
	sum(case when s.start_date>first_card_created_date and status = 'ACTIVE' then s.subscription_value end) as active_card_subscription_value,
	sum(case when s.start_date>first_card_created_date then s.committed_sub_value end) as acquired_committed_card_subscription_value,
	sum(case when s.start_date>first_card_created_date and status = 'ACTIVE' then s.committed_sub_value end) as active_committed_card_subscription_value
from
	(
	select
		customer_id,
		subscription_id,
		subscription_value_euro as subscription_value,
		subscriptions_per_customer,
		rental_period,
		cancellation_date,
		committed_sub_value,
		start_date,
		status,
		first_value (start_date) over (partition by customer_id
	order by
		start_date rows between unbounded preceding and current row) as first_subscription_start_date
	from
		ods_production.subscription)s
left join (
	select
		distinct customer_id,
		first_value(first_event_timestamp) over (partition by customer_id
	order by
		first_event_timestamp asc
                    rows between unbounded preceding and unbounded following) as first_card_created_date
	from
	where
		first_event_name = 'created')gc2
    on
	s.customer_id = gc2.customer_id
group by
	1,2,3,4 )
,tax_id as (
select
	customer_id,
	min(tax_id_submitted_date) as tax_id_submitted_date
from
group by
	1)
,
seizures as (
select
	customer_id,
	min(date)::Date as first_seizure_date,
	sum(amount) as seizure_amount
from
group by
	1)
,
active_subscriptions as (
select
	gc2.customer_id,
	first_card_created_date,
	sum(aso.active_subscriptions) as active_subscriptions_before_card,
	sum(aso.active_subscription_value) as active_subscription_value_before_card,
	sum(aso.active_committed_subscription_value) as active_committed_subscription_value_before_card
from
	(
	select
		distinct customer_id,
		first_value(first_event_timestamp) over (partition by customer_id
	order by
		first_event_timestamp asc
                    rows between unbounded preceding and unbounded following) as first_card_created_date
	from
	where
		first_event_name = 'created')gc2
inner join ods_finance.active_subscriptions_overview aso 
    ON aso.customer_id = gc2.customer_id
	and first_card_created_date::Date = aso.fact_date
group by
	1,
	2
    )
,
crm_label as (
select
	gc2.customer_id,
	ch.crm_label as crm_label_before_card,
	Round(avg(sh.rental_period), 2) as avg_rental_period_before_card
from
	master.customer_historical ch
inner join (
	select
		distinct customer_id,
		first_value(first_event_timestamp) over (partition by customer_id
	order by
		first_event_timestamp asc
                    rows between unbounded preceding and unbounded following) as first_card_created_date
	from
	where
		first_event_name = 'created')gc2
    ON gc2.customer_id = ch.customer_id
	and gc2.first_card_created_date::Date = ch."date"
left join master.subscription_historical sh
    ON gc2.customer_id = sh.customer_id
	and gc2.first_card_created_date::Date = sh."date"
group by
	1,
	2)   
,
pageview as (
select
	gc.customer_id::integer,
	count(case when page_view_date<first_event_timestamp then session_id end) sessions_before_card,
	count(case when page_view_date>first_event_timestamp then session_id end) sessions_after_card,
	count(case when page_view_date>first_event_timestamp and 
        then session_id end) sessions_for_card_page,
	sum(case when page_view_date<first_event_timestamp then time_engaged_in_s end) time_engaged_before_card,
	sum(case when page_view_date>first_event_timestamp then time_engaged_in_s end) time_engaged_after_card,
	sum(case when page_view_date>first_event_timestamp and 
   then time_engaged_in_s end) time_engaged_for_card_page
from
left join (
	select
		*
	from
		traffic.page_views
	where
		isnumeric(customer_id)= true) s3 
    on
	s3.customer_id = gc.customer_id
group by
	1)
,
cash as (
select
	distinct customer_id::integer,
	first_value (event_timestamp) over (partition by customer_id
order by
	event_timestamp rows between unbounded preceding and unbounded following) as First_redeem,
from
where
	event_name = 'Redemption' 
         )
,
payments as (
select
	customer_id,
	count(distinct case when payment_type = 'FIRST' and status = 'PAID' then subscription_payment_id end ) as first_card_payment,
	count(distinct case when payment_type = 'RECURRENT' and status = 'PAID' then subscription_payment_id end ) as recurrent_card_payment,
	count(distinct case when payment_type = 'RECURRENT' and status = 'FAILED' then subscription_payment_id end ) as Failed_card_payment
from
where
	first_card_created_date<due_Date
group by
	1)
,
mau_status as (
select 
		month_,
		customer_id ,
		mau_status
from
where
	month_ = dateadd('month',-1,date_trunc('month', current_date)::Date)
	)
select
	distinct c.customer_id,
	p.external_user_id as solaris_id,
	customer_type,
	wl.created_at as waiting_list_date,
	p.accepted_tnc_at,
	p.created_at as user_created_date,
	p.last_event_timestamp as user_screening_date,
	gr.card_requested_date as first_card_requested_date,
	cd.first_card_created_date as first_card_created_date,
	cd.first_activated_timestamp as First_activated_date,
	ti.tax_id_submitted_date,
	sz.first_seizure_date,
	datediff(day,gr.card_requested_date::timestamp,cd.first_card_created_date::timestamp) as days_to_onboard,
	ts.first_subscription_start_date,
	ts.first_subscription_start_date_after_card,
	ts.max_cancellation_date_before_card,
	ac.legal_closure_date as account_closure_date,
	ac.closure_reason,
	tu.first_timestamp_money_received,
	tu.first_timestamp_payment_successful,
	tu.first_timestamp_atm_withdrawal,
	tu.first_timestamp_transaction_refund,
	ch.First_redeem,
	u.event_journey as user_event_journey,
	i.status_journey as user_identification_status,
	cd.card_first_event as active_card_first_event,
	cd.card_last_event as Active_card_last_event,
	i.event_status_final as status_final_identification,
	seizure_amount,
	cd.total_card_count as total_cards,
	tu.amount_transaction_atm_withdrawal,
	tu.amount_transaction_card_transaction_refund,
	tu.amount_transaction_money_received,
	tu.amount_transaction_payment_successful,
	ts.subscriptions_per_customer ,
	total_subscription_before_card,
	ts.total_card_subscriptions,
	ts.active_subscriptions,
	ts.active_subscription_value,
	a_sub.active_subscriptions_before_card,
	active_card_subscriptions,
	total_subscription_value_before_card,
	total_card_subscription_value,
	acquired_committed_card_subscription_value,
	a_sub.active_subscription_value_before_card,
	active_card_subscription_value,
	active_committed_card_subscription_value,
	a_sub.active_committed_subscription_value_before_card,
	case
		when total_card_subscriptions>0 then true
		else false
	end as is_total_after_card,
	case
		when active_card_subscriptions>0 then true
		else false
	end as is_active_after_card,
	avg_rental_period_before_card,
	round(avg_card_rental_period, 2) as avg_card_rental_period,
	first_card_payment,
	recurrent_card_payment,
	ch.Grover_cash,
	pg.sessions_before_card,
	pg.sessions_after_card,
	pg.sessions_for_card_page,
	Round(pg.time_engaged_before_card / coalesce (sessions_before_card, 0), 2) as Avg_time_engaged_before_card,
	Round(pg.time_engaged_after_card / coalesce (sessions_after_card, 0), 2) as Avg_time_engaged_after_card,
	Round(pg.time_engaged_for_card_page / coalesce(sessions_for_card_page, 0), 2) as avg_time_engaged_for_card_page,
	cl.crm_label_before_card,
	case
		when coalesce(od.submitted_orders, 0)<= 0 then 'Registered'
		when coalesce(s1.active_subscriptions, 0)>= 1 then 'Active'
		when coalesce(s1.active_subscriptions, 0)= 0
		and max_cancellation_date >= current_date::date-30 * 6 then 'Inactive'
		when coalesce(s1.active_subscriptions, 0)= 0
		and max_cancellation_date<current_date::date-30 * 6 then 'Lapsed'
		else 'Passive'
	end as crm_label,
	case
		when cd.first_card_created_date is not null then crm_label
	end as current_crm_label,
	case
		when datediff('DAY',
		user_created_date::timestamp)<1 then 1
		else 0
	end as new_customer,
	case
		when tu.is_active>0 then true
		else false
	end as is_active_card,
	ms.mau_status,
	gc.user_classification
from
	ods_production.customer c
	c.customer_id = wl.customer_id
left join card_requested gr on
	c.customer_id = gr.customer_id
left join card cd on
	c.customer_id = cd.customer_id
	c.customer_id = p.customer_id
left join total_subscriptions ts on
	c.customer_id = ts.customer_id
left join active_subscriptions a_sub on
	c.customer_id = a_sub.customer_id
	u.customer_id = c.customer_id
	tu.customer_id = c.customer_id
left join pageview pg on
	pg.customer_id = c.customer_id
	c.customer_id = i.customer_id
left join cash ch on
	c.customer_id = ch.customer_id
left join crm_label cl on
	c.customer_id = cl.customer_id
left join payments ps on
	ps.customer_id = c.customer_id
left join tax_id ti on
	ti.customer_id = c.customer_id
left join seizures sz on
	c.customer_id = sz.customer_id
left join ods_production.customer_subscription_details s1 on
	s1.customer_id = c.customer_id
left join ods_production.customer_orders_details od on
	od.customer_id = c.customer_id
	ac.customer_id = c.customer_id
left join mau_status ms on
	ms.customer_id = c.customer_id
	gc.customer_id=c.customer_id
where
	(wl.created_at is not null
		or gr.card_requested_date is not null
		or p.created_at is not null)
	and c.customer_type = 'normal_customer';