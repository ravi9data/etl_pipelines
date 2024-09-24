drop table if exists b2b_sync_asv;
CREATE TEMP TABLE b2b_sync_asv as	
with 
classification as (
select 
	DATEADD('day',-1,DATE_TRUNC('month',datum)) as prev_month,
	DATEADD('day',-1,DATEADD('month',-1,DATE_TRUNC('month',datum))) as prev_2_month,
	DATEADD('day',-1,DATEADD('month',-2,DATE_TRUNC('month',datum))) as prev_3_month,
	DATEADD('day',-1,DATEADD('month',-3,DATE_TRUNC('month',datum))) as prev_4_month,
	datum as fact_day,
	datum-1 as yesterday,
	datum-2 as prev_2_days,
	datum-7 as prev_7_days
	from public.dim_dates
where datum = CURRENT_DATE 
),
dates as (
select distinct 
 datum as fact_date, 
 date_trunc('month',datum)::DATE as month_bom,
 LEAST(DATE_TRUNC('MONTH',DATEADD('MONTH',1,DATUM))::DATE-1,CURRENT_DATE) AS MONTH_EOM,
 c.*
from public.dim_dates d ,classification c
where datum <= current_date
ORDER BY 1 DESC 
)
,active_subs as (
select 
 fact_date,
 customer_id,
 prev_month,
 prev_2_month,
 prev_3_month,
 prev_4_month,
 yesterday,
 prev_2_days,
 prev_7_days,
 count(distinct s.subscription_id) as active_subscriptions,
 sum(s.subscription_value_eur)::decimal(10,2) as active_subscription_value,
 sum(s.subscription_value_eur * minimum_term_months) as active_committed_sub_value
  from dates d
left join (select s.subscription_id,s.store_label, s.store_commercial, s.customer_id, c2.company_type_name, s.subscription_value_eur,s.end_date,ss.minimum_term_months, s.fact_day
 from ods_production.subscription_phase_mapping s 
 left join ods_production.companies c2 
 on c2.customer_id = s.customer_id
 left join ods_production.subscription ss 
 on s.subscription_id = ss.subscription_id
 where s.customer_type = 'business_customer'
 ) s 
on d.fact_date::date >= s.fact_Day::date and
  d.fact_date::date <= coalesce(s.end_date::date, d.fact_date::date+1)
group by 1,2,3,4,5,6,7,8,9
order by 1 desc
)
,max_asv_pre as (
select 
	*,
	max(active_subscription_value) over (partition by customer_id) as max_asv,
	rank() over (partition by customer_id,active_subscription_value order by fact_date ASC) as idx,
	min(case when active_subscription_value > 350 then fact_date end ) over (partition by customer_id order by fact_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_time
	from active_subs )
	,max_asv as(
	select *
	from max_asv_pre where idx = 1 
	and (active_subscription_value = max_asv) 
	) 
	,subs as (
	select 	s.customer_id, 
	        sum(case when minimum_cancellation_date> current_date and minimum_cancellation_date< DATEADD('day',7,DATE_TRUNC('day',current_date)) then subscription_value end)as upcoming_7_days_churn_asv,
			sum(case when minimum_cancellation_date> current_date and minimum_cancellation_date< DATEADD('month',1,DATE_TRUNC('month',current_date)) then subscription_value end)as upcoming_churn_asv,
            sum(case when minimum_cancellation_date>= DATEADD('month',1,DATE_TRUNC('month',current_date)) and minimum_cancellation_date< DATEADD('month',2,DATE_TRUNC('month',current_date)) then subscription_value end)as upcoming_next_month_churn_asv,			
			sum(case when minimum_cancellation_date>= DATEADD('month',2,DATE_TRUNC('month',current_date)) and minimum_cancellation_date< DATEADD('month',3,DATE_TRUNC('month',current_date)) then subscription_value end)as upcoming_2_month_churn_asv,
            sum(case when minimum_cancellation_date>= DATEADD('month',3,DATE_TRUNC('month',current_date)) and minimum_cancellation_date< DATEADD('month',4,DATE_TRUNC('month',current_date)) then subscription_value end)as upcoming_3_month_churn_asv,
			sum(case when minimum_cancellation_date> current_date and minimum_cancellation_date< DATEADD('day',7,DATE_TRUNC('day',current_date)) then subscription_value*minimum_term_months end)as upcoming_7_days_churn_csv,
            sum(case when minimum_cancellation_date> current_date and minimum_cancellation_date< DATEADD('month',1,DATE_TRUNC('month',current_date)) then subscription_value*minimum_term_months end)as upcoming_churn_csv,
            sum(case when minimum_cancellation_date>= DATEADD('month',1,DATE_TRUNC('month',current_date)) and minimum_cancellation_date< DATEADD('month',2,DATE_TRUNC('month',current_date)) then subscription_value*minimum_term_months end)as upcoming_next_month_churn_csv,			
			sum(case when minimum_cancellation_date>= DATEADD('month',2,DATE_TRUNC('month',current_date)) and minimum_cancellation_date< DATEADD('month',3,DATE_TRUNC('month',current_date)) then subscription_value*minimum_term_months end)as upcoming_2_month_churn_csv,
            sum(case when minimum_cancellation_date>= DATEADD('month',3,DATE_TRUNC('month',current_date)) and minimum_cancellation_date< DATEADD('month',4,DATE_TRUNC('month',current_date)) then subscription_value*minimum_term_months end)as upcoming_3_month_churn_csv        
            from ods_production.subscription s 
left join ods_production.customer c2 
 on c2.customer_id = s.customer_id
where s.status = 'ACTIVE'
and  customer_type = 'business_customer'
group by 1)
,filtering as (
select 
*
from active_subs  a 
where ((fact_date = yesterday) or 
	 (fact_date = prev_month) or 
	 (fact_date = prev_2_month) or 
	 (fact_date = prev_3_month)	 or 
	 (fact_date = prev_4_month)	 or 
	 (fact_date = prev_2_days)	or
	  (fact_date = prev_7_days)
	 )
order by 1 desc )
,asv as (
select 
	customer_id,
	sum(case when fact_date = yesterday then active_subscription_value end)  as  live_asv,
	sum(case when fact_date = prev_7_days then active_subscription_value end)  as  live_asv_prev_7_days,
	sum(case when fact_date = prev_2_days then active_subscription_value end)  as  live_asv_prev_2_days,
	sum(case when fact_date = prev_month then active_subscription_value end) as live_asv_prev_month,
	sum(case when fact_date = prev_2_month then active_subscription_value end) as live_asv_prev_2_month,
    sum(case when fact_date = prev_3_month then active_subscription_value end) as live_asv_prev_3_month,
    sum(case when fact_date = prev_4_month then active_subscription_value end) as live_asv_prev_4_month,
    sum(case when fact_date = yesterday then active_committed_sub_value end)  as  live_csv,
    sum(case when fact_date = prev_2_days then active_committed_sub_value end)  as  live_csv_prev_2_days,
    sum(case when fact_date = prev_month then active_committed_sub_value end) as live_csv_prev_month,
	sum(case when fact_date = prev_2_month then active_committed_sub_value end) as live_csv_prev_2_month,
    sum(case when fact_date = prev_3_month then active_committed_sub_value end) as live_csv_prev_3_month,
    sum(case when fact_date = prev_4_month then active_committed_sub_value end) as live_csv_prev_4_month
	from filtering 
	group by 1)
	select a.*
	,upcoming_7_days_churn_asv
	,upcoming_churn_asv
	,upcoming_next_month_churn_asv
	,upcoming_2_month_churn_asv
	,upcoming_3_month_churn_asv
	,upcoming_7_days_churn_csv
	,upcoming_churn_csv
	,upcoming_next_month_churn_csv
	,upcoming_2_month_churn_csv
	,upcoming_3_month_churn_csv
	,max_asv
	,m.first_time
	from asv a 
	left join subs s on a.customer_id = s.customer_id
	left join max_asv m on a.customer_id = m.customer_id;


	drop table if exists b2b_sync_subs_1;
	CREATE TEMP TABLE b2b_sync_subs_1 as
	with min_cancellation_Date as ( 
	select case when minimum_cancellation_date < current_date then 
										case when DATE_PART('day',minimum_cancellation_date) > DATE_PART('day',CURRENT_DATE) then DATEADD('day',DATE_PART('day',minimum_cancellation_date)::int -1,date_trunc('month',current_date))
										     else  DATEADD('day',DATE_PART('day',minimum_cancellation_date)::int-1,dateadd('month',1,date_trunc('month',current_date))) end 
										 else minimum_cancellation_date end as next_cancellation_date,    
		minimum_cancellation_date ,minimum_cancellation_date,* from ods_production.subscription 
		where status = 'ACTIVE')
	,last_cancellation_date as (
			select customer_id, 
			max(Cancellation_date)::date as last_cancellation_date
			from ods_production.subscription 
			where status = 'CANCELLED'
			group by 1)
	,next_churn_date as (
			select customer_id,
			min(next_cancellation_date)::Date as next_cancellation_Date,
			count(distinct case when status = 'ACTIVE' then order_id end) as active_orders,
			count(*) as active_subs
			from min_cancellation_date
			group by 1)
	select nc.*,
	lc.last_cancellation_date
	from next_churn_date nc
	left join last_cancellation_date lc on lc.customer_id = nc.customer_id;
	
drop table if exists b2b_sync_subs_2;
	CREATE TEMP TABLE b2b_sync_subs_2 as
	with subs_metrics as (
			select order_id,
			s.customer_id,
			start_date::Date as first_time,
			sum(subscription_value) as sub_value, 
			sum(committed_sub_value) as committed_sub_value, 
			sum(outstanding_subscription_revenue) as open_invoice_amount,
			sum(case when DATE_TRUNC('quarter',start_date) = DATE_TRUNC('quarter',current_date) then subscription_value end) as this_quarter_acquired_sv,
			sum(case when DATE_TRUNC('quarter',start_date) = DATEADD('quarter',-1,DATE_TRUNC('quarter',current_date)) then subscription_value end) as last_quarter_acquired_sv,
			sum(case when DATE_TRUNC('quarter',start_date) = DATE_TRUNC('quarter',current_date) then minimum_term_months*subscription_value end) as this_quarter_csv,
			sum(case when DATE_TRUNC('quarter',start_date) = DATEADD('quarter',-1,DATE_TRUNC('quarter',current_date)) then minimum_term_months*subscription_value end) as last_quarter_csv
		from ods_production.subscription s 
		left join ods_production.subscription_cashflow cf on cf.subscription_id = s.subscription_id
		group by 1,2,3)
		,first_time_asv_350 as (
		select customer_id,min(first_time) as first_time
		from subs_metrics where sub_value >= 350
		group by 1)
		,subs as (
		select customer_id,
		sum(sub_value) as sub_value,
		sum(committed_sub_value) as committed_sub_value,
		sum(open_invoice_amount) as open_invoice_amount,
		sum(this_quarter_acquired_sv) as this_quarter_acquired_sv,
		sum(last_quarter_acquired_sv) as last_quarter_acquired_sv,
		sum(this_quarter_csv) as this_quarter_csv,
		sum(last_quarter_csv) as last_quarter_csv
		from subs_metrics 
		group by 1
		)
		select s.*
		from subs s;
		
	drop table if exists b2b_sync_orders;
		CREATE TEMP TABLE b2b_sync_orders as 	
		with order_pre as (
			select *,
			dense_rank() over (partition by customer_id order by submitted_date ASC) as order_rank_new,
			ROW_NUMBER() over (partition by customer_id order by submitted_date dESC) as idx
			from master."order" where  submitted_date is not null 
	       and status not in ('DECLINED','FAILED FIRST PAYMENT','CANCELLED'))
	       select 
		       customeR_id, 
		       max(case when order_rank_new =1 then submitted_date::Date end) date_1st_order,
		       max(case when order_rank_new =2 then submitted_date::Date end) date_2nd_order,
	           max(case when order_rank_new =3 then submitted_date::Date end) date_3rd_order,
	           max(case when order_rank_new =5 then submitted_date::Date end) date_5th_order,
	           max(case when order_rank_new =10 then submitted_date::Date end) date_10th_order,
	           sum(case when order_rank_new =1 then order_value * avg_plan_duration end) csv_1st_order,
		       sum(case when order_rank_new =2 then order_value * avg_plan_duration end) csv_2nd_order,
	           sum(case when order_rank_new =3 then order_value *avg_plan_duration end) csv_3rd_order,
	           sum(case when order_rank_new =5 then order_value * avg_plan_duration end) csv_5th_order,
	           sum(case when order_rank_new =10 then order_value * avg_plan_duration end) csv_10th_order,
	           max(case when idx = 1 then order_id end ) as last_order_id,
	           max(case when idx = 1 then submitted_date::date end) as last_order_date,
	           max(case when idx = 1 then payment_method end) as last_payment_method
	           from order_pre 
	           group by 1;
	          
	          drop table if exists b2b_sync;
	          create temp table b2b_sync as 
	          select 
		          ac.account_id
		          ,b.customer_id
	          	,c.status::varchar(100) as risk_status
	          	,live_asv::int
	          	,live_asv_prev_2_days::int
	          	,live_asv_prev_7_days::int
				,live_asv_prev_month::int
				,live_asv_prev_2_month::int
				,live_asv_prev_3_month::int
				,live_asv_prev_4_month::int
				,live_csv::int
				,live_csv_prev_2_days::int
				,live_csv_prev_month::int
				,live_csv_prev_2_month::int
				,live_csv_prev_3_month::int
			    ,live_csv_prev_4_month::int
				,upcoming_7_days_churn_asv::int
				,upcoming_churn_asv::int
				,upcoming_next_month_churn_asv::int
				,upcoming_2_month_churn_asv::int
				,upcoming_3_month_churn_asv::int
				,upcoming_7_days_churn_csv::int
				,upcoming_churn_csv::int
				,upcoming_next_month_churn_csv::int
				,upcoming_2_month_churn_csv::int
				,upcoming_3_month_churn_csv::int
				,max_asv::int
	            ,next_cancellation_date::date
				,active_orders::int
				,active_subs::int
				,last_cancellation_date::date
				,sub_value::int 
				,committed_sub_value::int
				,open_invoice_amount::int
				,this_quarter_acquired_sv::int
				,last_quarter_acquired_sv::int
				,this_quarter_csv::int
				,last_quarter_csv::int
				,first_time::date
				,date_1st_order::date
				,date_2nd_order::date
				,date_3rd_order::date
				,date_5th_order::date
				,date_10th_order::date
				,csv_1st_order::int
				,csv_2nd_order::int
				,csv_3rd_order::int
				,csv_5th_order::int
				,csv_10th_order::int
				,last_order_id::varchar(50)
				,last_order_date::date
				,last_payment_method::varchar(100)
	          from b2b_sync_asv b 
	          left join b2b_sync_subs_1 bs1 on b.customer_id = bs1.customer_id 
	          left join b2b_sync_subs_2 bs2 on b.customer_id = bs2.customer_id 
	         left join b2b_sync_orders bo on b.customer_id = bo.customer_id 
	         left join ods_production.companies c on b.customer_id = c.customer_id
	        left join ods_b2b.account ac on ac.customer_id = b.customer_id;
            
            drop table if exists dm_b2b.b2b_sync_account;
            create table dm_b2b.b2b_sync_account as 
            select * 
            from b2b_sync ac
	       where ac.account_id is not null;



            drop table if exists dm_b2b.lead_sync;
	        create table dm_b2b.lead_sync as 
	        select l.lead_id,
	        l.customer_id, 
	        live_asv, 
	        live_csv,
	        sub_value,
	        committed_sub_value
	        from b2b_sync b 
	        left join ods_b2b."lead" l on b.customer_id = l.customer_id
	        where l.lead_id is not null 
		        and l.converted_date is null  
		       ;