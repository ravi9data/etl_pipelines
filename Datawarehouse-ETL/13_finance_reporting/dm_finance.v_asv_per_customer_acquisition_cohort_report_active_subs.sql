---ASV_per_customer_acquisition_cohort_report_active_subs
drop view if exists dm_finance.v_asv_per_customer_acquisition_cohort_report;
create view dm_finance.v_asv_per_customer_acquisition_cohort_report as
select 
sh."date", 
date_trunc('month',sh.customer_acquisition_cohort) as customer_acquisition_cohort,
sum(case when sh.status='ACTIVE' then subscription_value end) as asv
from master.subscription_historical sh
inner join public.dim_dates d 
 on d.datum=sh.date 
where d.day_is_last_of_month=1 
group by 1,2
with no schema binding;