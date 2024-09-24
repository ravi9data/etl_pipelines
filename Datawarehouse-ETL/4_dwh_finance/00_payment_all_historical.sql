Begin;
create table trans_dev.s10_campaign_payment_historical as 
with a as (
select
	s.date,
	s.subscription_id,
	s.subscription_value,
	s.rental_period,
	s.customer_id,
	s.order_id,
	s.start_date,
	s.product_name,
	sp.payment_number,
	sp.next_payment_number,
	oi.pricing_campaign_id
from
	(
	select
		sp.*,
		lead(payment_number) over (partition by subscription_id,
		"date"
	order by
		payment_number) as next_payment_number
	from
		master.subscription_payment_historical sp
	left join public.dim_dates d on
		d.datum = sp.date
	where
		(d.day_is_last_of_month
			or d.datum = CURRENT_DATE-2
			or d.datum = CURRENT_DATE-1) ) sp
left join master.subscription_historical s on
	s.subscription_id = sp.subscription_id
	and s.date = sp.date
left join ods_production.order_item oi on
	oi.variant_sku = s.variant_sku
	and oi.order_id = s.order_id
where
	lower(s.product_name) like ('%galaxy s10%')
	and ((payment_number = 1
		and next_payment_number = 5)
	or (payment_number = 4
		and next_payment_number = 9))
	and pricing_campaign_id is not null) ,
	six as (
	select
		6 as rental_period,
		2 as payment_number,
		0 as amount_due
union all
	select
		6 as rental_period,
		3 as payment_number,
		0 as amount_due
union all
	select
		6 as rental_period,
		4 as payment_number,
		0 as amount_due
union all
	select
		12 as rental_period,
		5 as payment_number,
		0 as amount_due
union all
	select
		12 as rental_period,
		6 as payment_number,
		0 as amount_due
union all
	select
		12 as rental_period,
		7 as payment_number,
		0 as amount_due
union all
	select
		12 as rental_period,
		8 as payment_number,
		0 as amount_due)
select
	distinct a.date,
	a.subscription_id,
	a.subscription_value,
	a.order_id,
	a.customer_id,
	s.payment_number,
	a.start_date::date + 31 *(s.payment_number-1) as due_date,
	s.amount_due
from
	a
full outer join six s on
	s.rental_period = a.rental_period
where
	a.subscription_id is not null
order by
	a.subscription_id,
	s.payment_number
;


create table trans_dev.double_charge_fix_historical as 
with a as (
select
	ps.date,
	subscription_id,
	allocation_id,
	payment_number,
	date_trunc('month', due_date) as due_month,
	count(distinct payment_id) as count
from
	master.subscription_payment_historical ps
left join public.dim_dates d on
	d.datum = ps.date
where
	(d.day_is_last_of_month
		or d.datum = CURRENT_DATE-2
		or d.datum = CURRENT_DATE-1)
group by
	1,
	2,
	3,
	4,
	5
having
	count(distinct payment_id)>1) ,
	b as (
	select
		ps.payment_id as subscription_payment_id,
		ps.payment_sfid as subscription_payment_name,
		a.*
	from
		a
	inner join master.subscription_payment_historical ps on
		ps.subscription_id = a.subscription_id
		and ps.allocation_id = a.allocation_id
		and ps.payment_number = a.payment_number
		and a.due_month = date_trunc('month', ps.due_date)
			and a.date = ps.date
		where
			ps.amount_refund is null
)
select
	b.date,
	subscription_payment_id,
	subscription_payment_name,
	'double-charge' as is_double_charge,
	count(*)
from
	b
group by
	1,
	2,
	3
order by
	3 desc;

truncate table master.payment_all_historical;

insert into master.payment_all_historical
	select
	sp.date,
	sp.order_id::varchar,
	sp.customer_id::varchar,
	sp.subscription_id,
	sp.asset_id,
	sp.payment_id::varchar,
	sp.psp_reference,
	sp.status::varchar as status,
	sp.payment_type::varchar,
	sp.due_date::date,
	sp.paid_date::date,
	sp.amount_due::float,
	sp.amount_paid::float,
	coalesce(sp.amount_discount, 0)+ coalesce(sp.amount_voucher, 0) as discount_amount,
	dc.is_double_charge,
	sp2.tax_rate
from
	master.subscription_payment_historical sp
left join master.subscription_payment sp2 on
	sp.payment_id = sp2.payment_id
left join trans_dev.double_charge_fix_historical dc on
	dc.date = sp.date
	and dc.subscription_payment_id = sp.payment_id
left join public.dim_dates d on
	d.datum = sp.date
where
	(d.day_is_last_of_month
		or d.datum = CURRENT_DATE-2
		or d.datum = CURRENT_DATE-1) 
UNION ALL
	select
	ap.date,
	ap.order_id::varchar,
	ap.customer_id::varchar,
	ap.subscription_id,
	ap.asset_id,
	ap.asset_payment_id::varchar as payment_id,
	ap.psp_reference_id as psp_reference,
	ap.status::varchar as status,
	ap.payment_type::varchar,
	ap.due_date::date,
	ap.paid_date::date,
	ap.amount_due::float,
	ap.amount_paid::float,
	null as discount_amount,
	null as is_double_charge,
	coalesce(ap.tax_rate, 
	(case when ap.payment_type in ('COMPENSATION') then 0 
	when coalesce(st.country_name, st2.country_name, 'n/a') in ('Austria') and ap.payment_type in ('GROVER SOLD', 'CUSTOMER BOUGHT', 'DEBT COLLECTION') then 0.2 
	when coalesce(st.country_name, st2.country_name, 'n/a') in ('Netherlands') and ap.payment_type in ('GROVER SOLD', 'CUSTOMER BOUGHT', 'DEBT COLLECTION') then 0.21
	when coalesce(st.country_name, st2.country_name, 'n/a') in ('Spain') and ap.payment_type in ('GROVER SOLD', 'CUSTOMER BOUGHT', 'DEBT COLLECTION') then 0.21 
	when ap.paid_date::date between '2020-07-01' and '2020-12-31' then 0.16 when ap.paid_date::date < '2020-07-01' then 0.19 
	when ap.paid_date::date >= '2021-01-01' then 0.19 end) ) as vat_rate
from
	master.asset_payment_historical ap
left join ods_production.subscription s on
	ap.subscription_id = s.subscription_id
left join ods_production.store st on
	st.id = s.store_id
left join ods_production.order o on
	ap.order_id = o.order_id
left join ods_production.store st2 on
	st2.id = o.store_id
left join public.dim_dates d on
	d.datum = ap.date
where
	(d.day_is_last_of_month
		or d.datum = CURRENT_DATE-2
		or d.datum = CURRENT_DATE-1)  
UNION ALL
     select
	rp."date",
	rp.order_id::varchar,
	rp.customer_id::varchar,
	rp.subscription_id,
	rp.asset_id,
	rp.refund_payment_id::varchar payment_id,
	rp.psp_reference_id as psp_reference,
	rp.status::varchar as status,
	rp.refund_type::varchar,
	rp.created_at::date,
	rp.paid_date::date,
	rp.amount_due::float*-1 as amount_due,
	rp.amount_refunded::float*-1 as amount_paid ,
	null as discount_amount,
	null as is_double_charge,
	coalesce(rp.tax_rate, ps.tax_rate, 
	(case when pa.payment_type in ('COMPENSATION') then 0 
	when coalesce(st.country_name, st2.country_name, 'n/a') in ('Austria') and pa.payment_type in ('GROVER SOLD', 'CUSTOMER BOUGHT', 'DEBT COLLECTION') then 0.2 
	when coalesce(st.country_name, st2.country_name, 'n/a') in ('Netherlands') and pa.payment_type in ('GROVER SOLD', 'CUSTOMER BOUGHT', 'DEBT COLLECTION') then 0.21 
	when coalesce(st.country_name, st2.country_name, 'n/a') in ('Spain') and pa.payment_type in ('GROVER SOLD', 'CUSTOMER BOUGHT', 'DEBT COLLECTION') then 0.21 
	when rp.paid_date::date between '2020-07-01' and '2020-12-31' then 0.16 when rp.paid_date::date < '2020-07-01' then 0.19 
	when rp.paid_date::date >= '2021-01-01' then 0.19 
	when pa.paid_date::date between '2020-07-01' and '2020-12-31' then 0.16 
	when pa.paid_date::date < '2020-07-01' then 0.19 
	when pa.paid_date::date >= '2021-01-01' then 0.19 end) ) as tax_rate
from
	master.refund_payment_historical rp
left join ods_production.payment_asset pa on
	pa.asset_payment_id = rp.asset_payment_id
left join ods_production.payment_subscription ps on
	ps.subscription_payment_id = rp.subscription_payment_id
left join ods_production.subscription s on
	pa.subscription_id = s.subscription_id
left join ods_production.store st on
	st.id = s.store_id
left join ods_production.order o on
	pa.order_id = o.order_id
left join ods_production.store st2 on
	st2.id = o.store_id
left join ods_production.subscription s2 on
	ps.subscription_id = s2.subscription_id
left join ods_production.store st3 on
	st3.id = s2.store_id
left join public.dim_dates d on
	d.datum = rp.date
where
	(d.day_is_last_of_month
		or d.datum = CURRENT_DATE-2
		or d.datum = CURRENT_DATE-1) 
	UNION ALL 
	select
	sp.date,
	sp.order_id::varchar,
	sp.customer_id::varchar,
	sp.subscription_id,
	null as asset_id,
	'virtual'::varchar as payment_id,
	'virtual'::varchar as psp_reference,
	(case
		when sp.due_date >= current_date then 'PLANNED'::varchar
		else 'PAID'
	end)::varchar as status,
	'RECURRENT'::varchar as payment_type,
	sp.due_date::date,
	(case
		when sp.due_date >= current_date then null
		else sp.due_date
	end)::date as paid_date,
	sp.amount_due::float,
	(case
		when sp.due_date >= current_date then null
		else 0
	end)::float as amount_paid,
	sp.subscription_value as discount_amount,
	null as is_double_charge,
	null as tax_rate
from
	trans_dev.s10_campaign_payment_historical sp ;
End;
    
drop table if exists trans_dev.s10_campaign_payment_historical;
drop table if exists trans_dev.double_charge_fix_historical;

