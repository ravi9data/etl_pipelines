drop table if exists ods_finance.active_subscriptions_overview;
create table ods_finance.active_subscriptions_overview as
			select 
			datum as fact_date,
			date_trunc('week',f.datum)::date as fact_week, 
  			date_trunc('month',f.datum)::date as fact_month, 
			case 
				when week_day_number = 7 then 1
				when datum = current_date then 1
				else 0 end 
			as day_is_end_of_week,
  			day_is_last_of_month, 
			ss.customer_id,
			c.customer_type,  
			--b2b
			c.company_name, 
			c2.company_type_name, 
			bbfm.is_freelancer,
			--
			s2.country_name as store_country,
			s.store_short,
			s.store_commercial, 
			s.store_label, 
			s.store_name,
			case when datum > s.minimum_cancellation_date then true
			else false end as is_above_rental_plan,
			o.is_pay_by_invoice, 
			count (distinct ss.subscription_id) as active_subscriptions,
			sum(ss.subscription_value_eur) as active_subscription_value,
			sum(s.subscription_value_euro * s.rental_period) as active_committed_subscription_value
		from public.dim_dates f 
		left join ods_production.subscription_phase_mapping ss
		   on f.datum::date >= ss.fact_day::date 
		   and F.datum::date <= coalesce(ss.end_date::date, f.datum::date+1)
		left join ods_production.subscription s
 			on s.subscription_id = ss.subscription_id
		left join ods_production.order o 
			on o.order_id = s.order_id
		left join ods_production.customer c 
			on c.customer_id = s.customer_id
		left join ods_production.store s2 
			on s.store_id = s2.id
		left join ods_production.companies c2 
			on c2.customer_id = s.customer_id
		left join dm_risk.b2b_freelancer_mapping bbfm 
			on bbfm.company_type_name = c2.company_type_name 
		where datum<=CURRENT_DATE::date
		and fact_date >= '2015-06-28'
		group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17;

GRANT SELECT ON ods_finance.active_subscriptions_overview TO tableau;
