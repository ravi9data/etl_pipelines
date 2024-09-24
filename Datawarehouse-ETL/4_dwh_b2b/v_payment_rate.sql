		create or replace view dm_b2b.v_payment_rate as
				select 
					distinct date_trunc('week', due_date)::date as week_due,
					case when c.consolidation_day !='' then true else false end as consolidation_customer,
					coalesce(cc.is_freelancer, 0) as is_freelancer,
					cc.is_gbp_enabled,
					case when o.is_pay_by_invoice then true else false end as is_pay_by_invoice,
					sp.customer_id, 
					cc.company_name,
					sum (case when datediff('day', sp.due_date, sp.paid_date) <= 30 and date_trunc('week', sp.due_date) < dateadd('week', -4, date_trunc('week', current_date)) then sp.amount_paid end) as amount_paid_30_rate_logic, 
					sum (case when datediff('day', sp.due_date, sp.paid_date) <= 14 then sp.amount_paid end) as amount_paid_14_rate_logic, 
					sum(sp.amount_due) as amount_due,
                    sum(sp.amount_paid) as amount_paid
					from master.subscription_payment sp 
					left join ods_production.customer c 
						on c.customer_id  = sp.customer_id 
					left join ods_production.companies cc
						on cc.customer_id = sp.customer_id 
					left join ods_production.order o 
						on o.order_id  = sp.order_id 
						and o.is_pay_by_invoice 
				where sp.customer_type ='business_customer'
				and sp.payment_type ='RECURRENT'
				and date_trunc('week', sp.due_date)::date >= '2021-01-01'
			group by 1, 2, 3, 4, 5, 6, 7
			WITH NO SCHEMA BINDING;