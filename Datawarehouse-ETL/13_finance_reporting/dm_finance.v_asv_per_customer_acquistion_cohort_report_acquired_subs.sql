---ASV_per_customer_acquisition_cohort_report_acquired_subs
drop view if exists dm_finance.v_asv_per_customer_acquisition_cohort_report_acquired_subs;
create view dm_finance.v_asv_per_customer_acquisition_cohort_report_acquired_subs as
select 
sh.start_date::date as start_date, 
date_trunc('month',sh.customer_acquisition_cohort) as customer_acquisition_cohort,
sum(subscription_value) as acquired_sub_value,
count(distinct subscription_id) as acquired_subs
from master.subscription sh
group by 1,2
with no schema binding;