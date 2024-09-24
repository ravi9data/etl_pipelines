drop view if exists dm_finance.v_due_dilligence_report_asset_details;
create view dm_finance.v_due_dilligence_report_asset_details as
select
    a.*,
    s.effective_duration,
    s.outstanding_duration,
    t.subscription_id as last_subscription_id,
    t.subscription_value as last_subscription_value,
    t.start_date as last_subscription_start_date,
    t.cancellation_date as last_subscription_end_date
from master.asset_historical a
    left join 
    master.subscription_historical s on 
    s."date"=a."date"
    and s.subscription_id=a.subscription_id
        left join public.dim_dates d 
        on d.datum=a."date"
            left join
            dm_finance.asset_subscription_eom_mapping t
            on t.asset_id = a.asset_id
            and t.datum= a.date
where 
(a."date"=CURRENT_DATE-1 or day_is_last_of_month=1) 
and a."date">date_trunc('month',dateadd(month,-6,CURRENT_DATE))
with no schema binding;