--asset table
delete from stg_salesforce.asset_history
where createddate::date>=(select min(createddate)::date from stg_salesforce_dl.assethistory WHERE createddate > current_date-7);

insert into stg_salesforce.asset_history
select id,isdeleted,assetid,createdbyid,createddate,field,oldvalue,newvalue
from stg_salesforce_dl.assethistory;

truncate stg_salesforce_dl.assethistory;

--allocation table
delete from stg_salesforce.customer_asset_allocation__history
where createddate::date>=(select min(createddate)::date from stg_salesforce_dl.customer_asset_allocation__history WHERE createddate > current_date-7);

insert into stg_salesforce.customer_asset_allocation__history
select id,isdeleted,parentid,createdbyid,createddate,field,oldvalue,newvalue
from stg_salesforce_dl.customer_asset_allocation__history;

truncate stg_salesforce_dl.customer_asset_allocation__history;

--asset payment table
delete from stg_salesforce.asset_payment_history
where createddate::date>=(select min(createddate)::date from stg_salesforce_dl.asset_payment__history WHERE createddate > current_date-7);

insert into stg_salesforce.asset_payment_history
select id,isdeleted,parentid,createdbyid,createddate,field,oldvalue,newvalue
from stg_salesforce_dl.asset_payment__history;


truncate stg_salesforce_dl.asset_payment__history;

--order table
delete from stg_salesforce.order_history
where createddate::date>=(select min(createddate)::date from stg_salesforce_dl.orderhistory WHERE createddate > current_date-7);

insert into stg_salesforce.order_history
select id,isdeleted,orderid,createdbyid,createddate,field,oldvalue,newvalue
from stg_salesforce_dl.orderhistory;

truncate stg_salesforce_dl.orderhistory;

--subscription table
delete from stg_salesforce.subscription_history
where createddate::date>=(select min(createddate)::date from stg_salesforce_dl.subscription__history WHERE createddate > current_date-7);

insert into stg_salesforce.subscription_history
select id,isdeleted,parentid,createdbyid,createddate,field,oldvalue,newvalue
from stg_salesforce_dl.subscription__history;

truncate stg_salesforce_dl.subscription__history;


--subscription payment table
delete from stg_salesforce.subscription_payment_history
where createddate::date>=(select min(createddate)::date from stg_salesforce_dl.subscription_payment__history WHERE createddate > current_date-7);

insert into stg_salesforce.subscription_payment_history
select id,isdeleted,parentid,createdbyid,createddate,field,oldvalue,newvalue
from stg_salesforce_dl.subscription_payment__history;

truncate stg_salesforce_dl.subscription_payment__history;
