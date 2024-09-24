--
-- Order
--
drop table if exists _order;
create temp table _order as 
with rd as (
	select "date" as fact_date,
		order_id  ,
		count(1) c
	from master.order_historical
	where date > current_date - 14
	group by 1, 2
	having c > 1
)
select fact_date,
	count(order_id) duplicated_id
from rd
group by 1;

--
-- Subscription
--
drop table if exists _subscription;
create temp table _subscription as 
with rd as (
	select "date" as fact_date,
		subscription_id,
		count(1) c
	from master.subscription_historical
	where date > current_date - 14
	group by 1, 2
	having c > 1
)
select fact_date,
	count(subscription_id) duplicated_id
from rd
group by 1;

--
-- Subscription Payment
--
drop table if exists _subscription_payment;
create temp table _subscription_payment as 
with rd as (
	select "date" as fact_date,
		payment_id,
		count(1) c
	from master.subscription_payment_historical
	where date > current_date - 14
	group by 1, 2
	having c > 1
)
select fact_date,
	count(payment_id) duplicated_id
from rd
group by 1;

--
-- Refund Payment
--
drop table if exists _refund_payment;
create temp table _refund_payment as 
with rd as (
	select "date" as fact_date,
		refund_payment_id,
		count(1) c
	from master.refund_payment_historical
	where date > current_date - 14
	group by 1, 2
	having c > 1
)
select fact_date,
	count(refund_payment_id) duplicated_id
from rd
group by 1;

--
-- Asset Payment
--
drop table if exists _asset_payment;
create temp table _asset_payment as 
with rd as (
	select "date" as fact_date,
		asset_payment_id,
		count(1) c
	from master.asset_payment_historical
	where date > current_date - 14
	group by 1, 2
	having c > 1
)
select fact_date,
	count(asset_payment_id) duplicated_id
from rd
group by 1;

--
-- Allocation
--
drop table if exists _allocation;
create temp table _allocation as 
with rd as (
	select "date" as fact_date,
		allocation_id,
		count(1) c
	from master.allocation_historical
	where date > current_date - 14
	group by 1, 2
	having c > 1
)
select fact_date,
	count(allocation_id) duplicated_id
from rd
group by 1;

--
-- Asset
--
drop table if exists _asset;
create temp table _asset as 
with rd as (
	select "date" as fact_date,
		asset_id ,
		count(1) c
	from master.asset_historical
	where date > current_date - 14
	group by 1, 2
	having c > 1
)
select fact_date,
	count(asset_id) duplicated_id
from rd
group by 1;

--
-- Customer
--
drop table if exists _customer;
create temp table _customer as 
with rd as (
	select "date" as fact_date,
		customer_id  ,
		count(1) c
	from master.customer_historical
	where date > current_date - 14
	group by 1, 2
	having c > 1
)
select fact_date,
	count(customer_id) duplicated_id
from rd
group by 1;

--
-- Variant
--
drop table if exists _variant;
create temp table _variant as 
with rd as (
	select "date" as fact_date,
		variant_id,
		count(1) c
	from master.variant_historical
	where date > current_date - 14
	group by 1, 2
	having c > 1
)
select fact_date,
	count(variant_id) duplicated_id
from rd
group by 1;


drop table if exists monitoring.master_duplicates;
create table monitoring.master_duplicates as
select 'Order' table_name, fact_date, duplicated_id from _order 
union
select 'Subscription' table_name, fact_date, duplicated_id from _subscription
union
select 'Subscription Payment' table_name, fact_date, duplicated_id from _subscription_payment
union
select 'Refund Payment' table_name, fact_date, duplicated_id from _refund_payment
union
select 'Asset Payment' table_name, fact_date, duplicated_id from _asset_payment
union
select 'Allocation' table_name, fact_date, duplicated_id from _allocation
union
select 'Asset' table_name, fact_date, duplicated_id from _asset
union
select 'Customer' table_name, fact_date, duplicated_id from _customer 
union
select 'Variant' table_name, fact_date, duplicated_id from _variant 
;



/*
DELETE from monitoring.master_count where "current_date"=current_date;
insert into monitoring.master_count
select 'asset',created_at,CURRENT_DATE::date as fact_date,asset_id,r/2 from(
SELECT created_at,	
	asset_id,
   ROW_NUMBER() OVER (PARTITION BY asset_id ) r
 FROM master.asset) where r>1;

insert into monitoring.master_count
select 
	'asset_historical',
	"date",
	CURRENT_DATE::date as fact_date,
	asset_id, 
	count(*)
from master.asset_historical
group by 2,3,4
having count(*)>1
order by 5 desc;

insert into monitoring.master_count
select 'customer',created_at,CURRENT_DATE::date as fact_date,customer_id,r/2 from(
SELECT created_at,	
	customer_id,
   ROW_NUMBER() OVER (PARTITION BY customer_id ) r
 FROM master.customer) where r>1;

insert into monitoring.master_count
select 
	'customer_historical',
	"date",
	CURRENT_DATE::date as fact_date,
	customer_id, 
	count(*)
from master.customer_historical
group by 2,3,4
having count(*)>1
order by 5 desc;

insert into monitoring.master_count
select 'order',created_date,CURRENT_DATE::date as fact_date,order_id,r/2 from(
SELECT created_date,	
	order_id,
   ROW_NUMBER() OVER (PARTITION BY order_id ) r
 FROM master."order") where r>1;

insert into monitoring.master_count
select
	'order_historical',
	"date",
	CURRENT_DATE::date as fact_date,
	order_id, 
	count(*)
from master.order_historical
group by 2,3,4
having count(*)>1
order by 5 desc;

insert into monitoring.master_count
select 'subscription',created_date,CURRENT_DATE::date as fact_date,subscription_id,r/2 from(
SELECT created_date,	
	subscription_id,
   ROW_NUMBER() OVER (PARTITION BY subscription_id ) r
 FROM master.subscription) where r>1;

insert into monitoring.master_count
select 
	'subscription_historical',
	"date",
	CURRENT_DATE::date as fact_date,
	subscription_id, 
	count(*)
from master.subscription_historical
group by 2,3,4
having count(*)>1
order by 5 desc;



insert into monitoring.master_count
select 'subscription_payment',subscription_start_date,CURRENT_DATE::date as fact_date,payment_id,r/2 from(
SELECT subscription_start_date,	payment_id,
   ROW_NUMBER() OVER (PARTITION BY payment_id ) r
 FROM master.subscription_payment) where r>1;



insert into monitoring.master_count
select 
	'subscription_payment_historical',
	"date",
	CURRENT_DATE::date as fact_date,
	payment_id, 
	count(*)
from master.subscription_payment_historical
group by 2,3,4
having count(*)>1
order by 5 desc;


insert into monitoring.master_count
select 'allocation',allocated_at,CURRENT_DATE::date as fact_date,allocation_id,r/2 from(
SELECT allocated_at,allocation_id,
   ROW_NUMBER() OVER (PARTITION BY allocation_id ) r
 FROM master.allocation) where r>1;



insert into monitoring.master_count
select 
	'allocation_historical',
	"date",
	CURRENT_DATE::date as fact_date,
	allocation_id, 
	count(*)
from master.allocation_historical
group by 2,3,4
having count(*)>1
order by 5 desc;

insert into monitoring.master_count
select 'refund_payment',created_at,CURRENT_DATE::date as fact_date,refund_payment_id,r/2 from(
SELECT created_at,refund_payment_id,
   ROW_NUMBER() OVER (PARTITION BY refund_payment_id ) r
 FROM master.refund_payment) where r>1;
 
 insert into monitoring.master_count
select 
	'refund_payment_historical',
	"date",
	CURRENT_DATE::date as fact_date,
	refund_payment_id, 
	count(*)
from master.refund_payment_historical
group by 2,3,4
having count(*)>1
order by 5 desc;

insert into monitoring.master_count
select 'asset_payment',created_at,CURRENT_DATE::date as fact_date,asset_payment_id,r/2 from(
SELECT created_at,asset_payment_id,
   ROW_NUMBER() OVER (PARTITION BY asset_payment_id ) r
 FROM master.asset_payment) where r>1;


insert into monitoring.master_count
select 
	'asset_payment_historical',
	"date",
	CURRENT_DATE::date as fact_date,
	asset_payment_id, 
	count(*)
from master.asset_payment_historical
group by 2,3,4
having count(*)>1
order by 5 desc;

insert into monitoring.master_count
select 'variant',variant_updated_at,CURRENT_DATE::date as fact_date,variant_sku,r/2 from(
SELECT variant_sku,variant_updated_at,
   ROW_NUMBER() OVER (PARTITION BY variant_sku ) r
 FROM master.variant) where r>1;

insert into monitoring.master_count
select 
	'variant_historical',
	"date",
	CURRENT_DATE::date as fact_date,
	variant_sku, 
	count(*)
from master.variant_historical
group by 2,3,4
having count(*)>1
order by 5 desc;
*/

GRANT SELECT ON monitoring.master_duplicates TO tableau;
