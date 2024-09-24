DELETE from monitoring.historic_monitoring;

INSERT into monitoring.historic_monitoring
select 'asset_history',datum 
from public.dim_dates 
where datum not in (select cast("date" as date) from master.asset_historical) 
and datum < getdate() 
and datum > (select cast(min("date") as date) from master.asset_historical) ;

INSERT into monitoring.historic_monitoring
select 'customer_history',datum 
from public.dim_dates 
where datum not in (select cast("date" as date) from master.customer_historical) 
and datum < getdate() 
and datum > (select cast(min("date") as date) from master.customer_historical) ;


INSERT into monitoring.historic_monitoring
select 'order_history',datum 
from public.dim_dates 
where datum not in (select cast("date" as date) from master.order_historical) 
and datum < getdate() 
and datum > (select cast(min("date") as date) from master.order_historical) ;

INSERT into monitoring.historic_monitoring
select 'subscription_history',datum 
from public.dim_dates 
where datum not in (select cast("date" as date) from master.subscription_historical) 
and datum < getdate() 
and datum > (select cast(min("date") as date) from master.subscription_historical) ;

INSERT into monitoring.historic_monitoring
select 'subscription_payment_historical',datum 
from public.dim_dates 
where datum not in (select cast("date" as date) from master.subscription_payment_historical) 
and datum < getdate() 
and datum > (select cast(min("date") as date) from master.subscription_payment_historical) ;

INSERT into monitoring.historic_monitoring
select 'refund_payment_historical',datum 
from public.dim_dates 
where datum not in (select cast("date" as date) from master.refund_payment_historical) 
and datum < getdate() 
and datum > (select cast(min("date") as date) from master.refund_payment_historical) ;

INSERT into monitoring.historic_monitoring
select 'asset_payment_historical',datum 
from public.dim_dates 
where datum not in (select cast("date" as date) from master.asset_payment_historical) 
and datum < getdate() 
and datum > (select cast(min("date") as date) from master.asset_payment_historical) ;

INSERT into monitoring.historic_monitoring
select 'variant_historical',datum 
from public.dim_dates 
where datum not in (select cast("date" as date) from master.variant_historical) 
and datum < getdate() 
and datum > (select cast(min("date") as date) from master.variant_historical) ;
