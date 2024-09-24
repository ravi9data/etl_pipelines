--payment_rate_historical_report
drop view if exists dm_finance.v_payment_rate_historical_report;
create view dm_finance.v_payment_rate_historical_report as
select sp.*
from master.subscription_payment_historical sp 
left join public.dim_dates d 
 on sp.date=d.datum
where date>= '2019-11-01'
 and (d.day_is_last_of_month=1 or datum::Date=current_date-1)
with no schema binding;