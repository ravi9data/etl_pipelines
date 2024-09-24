--Delete just in case
delete from dm_commercial.customer_data_tape_snapshots where snapshot_date = current_date;
--Insert
insert into dm_commercial.customer_data_tape_snapshots
(
with country_ as 
(
select customer_id, store_country, row_number() over (partition by customer_id order by created_date desc) row_n
from  master.order
)
, customer_info as (
select distinct c.customer_id,
c.customer_type,
	coalesce(cp.gender, 'n/a') as gender,
          case
              when cp.age < 18 then 'under 18'
              when cp.age between 18 and 22 then '[18-22]'
              when cp.age between 23 and 27 then '[23-27]'
              when cp.age between 28 and 32 then '[28-32]'
              when cp.age between 33 and 37 then '[33-37]'
              when cp.age between 38 and 42 then '[38-42]'
              when cp.age between 43 and 47 then '[43-47]'
              when cp.age between 48 and 52 then '[48-52]'
              when cp.age between 53 and 57 then '[53-57]'
              when cp.age between 58 and 62 then '[58-62]'
              when cp.age >= 63 then '> 62'
              else 'n/a'
          end  age,
              --cp.age,
    c.is_bad_customer,
	coalesce(country_.store_country, c.shipping_country, 'n/a') as shipping_country_,
	coalesce(c.shipping_city, 'n/a') as shipping_city,
	c.subscription_limit,
	case when country_.store_country != 'United States'
	    then   coalesce(c.burgel_risk_category, 'n/a')
         else coalesce(fico.score_mapped::text,'n/a')
	    end
    as risk_category,
	c.signup_language ,
	c.email_subscribe ,
	c.crm_label,
     case
         when country_.store_country != 'United States'
         then  c.created_at::date
        else (to_char(c.created_at::date,'YYYY-MM')||'-01')::date
         end
	    as register_date,
	c.customer_acquisition_cohort,
	c.clv
from master.customer c
left join country_
        on c.customer_id = country_.customer_id
        and country_.row_n = 1
left join ods_data_sensitive.customer_pii cp
on c.customer_id = cp.customer_id
    left join david.fico_test fico
    on c.customer_id = fico.customer_id
        and fico.rown = 1
)
,orders as
(select
distinct
	o.customer_id,
	o.store_label as first_order_store_label,
	o.store_name as first_order_store_name,
	omc.marketing_source  as first_order_marketing_source,
	o.device as first_order_device,
		min( case
         when  o.store_country != 'United States'
         then  o.created_date::date
        else (to_char(o.created_date::date,'YYYY-MM')||'-01')::date
         end
	    ) as first_order_date,

		min( case
         when  o.store_country != 'United States'
         then  t.created_date::date
        else (to_char(t.created_date::date,'YYYY-MM')||'-01')::date
         end
	    ) as first_order_date_paid,

		listagg(t.store_label, ',') as first_paid_order_store_label,
		listagg(t.store_name, ',') as first_paid_order_store_name,
		listagg(t.marketing_source, ',') as first_paid_order_marketing_source,
		listagg(t.device, ',') as first_paid_order_device
from master."order" o
left join ods_production.order_marketing_channel omc
on o.order_id = omc.order_id
left join (select customer_id, o.order_rank, o.store_label, o.store_name, marketing_source, device, o.created_date,
	rank() OVER (PARTITION BY customer_id ORDER BY paid_date asc) AS paid_date_rank 
	from master."order" o
	left join ods_production.order_marketing_channel omc
	 on o.order_id = omc.order_id
	 where paid_date is not NULL
	 ) as t
	 on t.customer_id = o.customer_id and t.paid_date_rank = 1
where o.order_rank = 1
and o.order_id != 'R647088467'
group by 1, 2, 3, 4, 5
)
,base_subscription as(
select 
s.customer_id,
subscription_id,
subscription_value,
created_date,
rental_period,
(committed_sub_value as additional_committed_sub_value) as committed_sub_value,
commited_sub_revenue_future,
subscription_revenue_paid,
net_subscription_revenue_paid,
delivered_assets,
status,
category_name,
subcategory_name,
product_sku,
product_name,
start_date,
rank_subscriptions,
country_name,
first_value(category_name) over(partition by s.customer_id ORDER BY created_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) first_cat_name, 
first_value(subcategory_name) over(partition by s.customer_id ORDER BY created_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) first_subcat_name,
first_value(brand)over(partition by s.customer_id ORDER BY created_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) first_brand_name
from master.subscription s
--inner join customer_info c
--on s.customer_id  = c.customer_id 
--where customer_id = '785518'
)
,subscriptions as (
select 
	distinct  
	customer_id, 
	first_cat_name,	
	first_subcat_name,	
	first_brand_name,	
	count ( subscription_id) as total_subscriptions,
	sum(subscription_value) as subscription_value,
	avg(rental_period)::int as avg_total_rental_plan,
	sum(committed_sub_value) as committed_sub_value,
	sum(commited_sub_revenue_future) as commited_sub_revenue_future,
	sum(subscription_revenue_paid) as total_subscription_revenue_paid,
	sum(net_subscription_revenue_paid) as total_net_subscription_revenue_paid,
	sum(delivered_assets) as delivered_subscriptions,
	count(case when status = 'ACTIVE' then subscription_id end) as active_subscriptions,
	sum(case when status = 'ACTIVE' then subscription_value end) as active_subscription_value,
		listagg( category_name::text,';') WITHIN GROUP (ORDER BY rank_subscriptions) as rented_category,
		listagg( subcategory_name::text,';') WITHIN GROUP (ORDER BY rank_subscriptions) as rented_subcategory,
		listagg( product_sku::text,';') WITHIN GROUP (ORDER BY rank_subscriptions) as rented_skus,
		--listagg( product_name::text,';') WITHIN GROUP (ORDER BY rank_subscriptions) as rented_product_name,
	    listagg(rental_period ,';') WITHIN GROUP (ORDER BY rank_subscriptions)  as rented_rental_period,
		listagg( case
         when  s.country_name != 'United States'
         then  start_date::date
        else (to_char(start_date::date,'YYYY-MM')||'-01')::date
         end ,';') WITHIN GROUP (ORDER BY rank_subscriptions)  as rented_start_date
		 from base_subscription s
		 group by 
		customer_id,
		first_cat_name,	
		first_subcat_name,	
		first_brand_name
)
,payment as(
select distinct p.customer_id,
	c.customer_acquisition_cohort,
	max(greatest(datediff(month, c.customer_acquisition_cohort::date, p.due_date::date),0))  as months_between_acquisition_due,
	max(due_date) as max_due_date,
	sum(p.amount_due) as amount_due
	from ods_production.payment_all p
	right join master.customer c
	on p.customer_id = c.customer_id
	where payment_type != 'DEBT COLLECTION' and c.customer_acquisition_cohort is not null
	and p.status not in ('CANCELLED')
	 and not (p.payment_type = 'FIRST' and p.status in ('FAILED','FAILED FULLY'))
	 and p.due_date::date < current_date-1
	group by 1, 2)
,collection as(
select distinct customer_id,
	customer_acquisition_cohort,
	max_due_date,
	date_part('year',customer_acquisition_cohort) as year_cohort,
	date_part('month',customer_acquisition_cohort) as month_cohort,
	count (distinct customer_id) as customer_count,
	max(months_between_acquisition_due) as max_months_between_acquisition,
	sum(amount_due) as amount_due
	from payment
	group by 1, 2, 3, 4)	
, final_collection as(
select
	year_cohort,
	month_cohort,
	((sum(amount_due) / sum(customer_count))) as earned_revenue_cohort,
	max(max_months_between_acquisition) as max_months_between_acquisition_due,
    sum(customer_count) as customer_count_cohort,
	sum(amount_due) as amount_due_cohort
	from collection
	group by 1, 2)	
,dhh as (
select distinct
        case
        when shipping_country_ != 'United States'
             then c.customer_id
            else (c.customer_id*400)-300
    end customer_id,
     current_date as snapshot_date,
   -- c.customer_id,
    c.is_bad_customer,
	c.customer_type,
	--gender,
	--
    age,
	shipping_country_,
	shipping_city,
	subscription_limit,
	risk_category,
	signup_language,
	email_subscribe,
	crm_label,
	register_date,
	clv,
	--ORDER
	first_order_date,
	COALESCE (o.first_order_store_label, '-') as first_order_store_label,
	COALESCE (o.first_order_store_name, '-') as first_order_store_name,
	COALESCE (o.first_order_marketing_source, '-') as first_order_marketing_source,
	COALESCE (o.first_order_device, '-') as first_order_device,
	o.first_order_date_paid,
	COALESCE (o.first_paid_order_store_label, '-') as first_paid_order_store_label,
	COALESCE (o.first_paid_order_marketing_source, '-') as first_paid_order_marketing_source,
	COALESCE (o.first_paid_order_device, '-') as first_paid_order_device,
	--SUBS
	COALESCE (s.total_subscriptions, 0) as total_subscriptions,
	COALESCE (s.active_subscriptions, 0) as active_subscriptions,
	COALESCE (s.subscription_value, 0) as subscription_value,
	COALESCE (s.active_subscription_value, 0) as active_subscription_value,
	COALESCE (s.avg_total_rental_plan, 0) as avg_total_rental_plan,
	COALESCE (s.committed_sub_value, 0) as committed_sub_value,
	COALESCE (s.commited_sub_revenue_future, 0) as commited_sub_revenue_future,
	COALESCE (s.total_subscription_revenue_paid, 0) as total_subscription_revenue_paid,
	COALESCE (s.total_net_subscription_revenue_paid, 0) as total_net_subscription_revenue_paid,
	COALESCE (s.delivered_subscriptions, 0) as delivered_subscriptions,
	--subscription active/non active
	s.rented_category,
	s.rented_subcategory,
	s.rented_skus,
	--s.rented_product_name,
	s.rented_rental_period,
	s.rented_start_date,
	--EARNED REVENUE
	c.customer_acquisition_cohort,
	co.max_months_between_acquisition_due,
  	co.customer_count_cohort,
	co.amount_due_cohort,
	co.earned_revenue_cohort,
	--EARNED REVENUE BY CUSTOMER ID
	cc.max_months_between_acquisition,
	cc.amount_due,
	s.first_cat_name,	
	s.first_subcat_name,	
	s.first_brand_name,	
	csd.subs_wearables,
	csd.subs_drones,
	csd.subs_cameras,
	csd.subs_phones_and_tablets,
	csd.subs_computers,
	csd.subs_gaming,
	csd.subs_audio,
	csd.subs_other
	from customer_info c
	left join orders o
		on c.customer_id = o.customer_id
	left join subscriptions s
		on s.customer_id = c.customer_id
	left join final_collection co
		on (co.year_cohort = date_part('year',c.customer_acquisition_cohort) and co.month_cohort =date_part('month',c.customer_acquisition_cohort))
	left join collection cc
		on cc.customer_id = c.customer_id
	left join ods_production.customer_subscription_details csd 	
		on c.customer_id = csd.customer_id
	)
	select * from dhh
	where customer_acquisition_cohort is not null
);