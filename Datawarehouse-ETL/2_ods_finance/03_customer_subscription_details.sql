DROP TABLE IF EXISTS customer_subscription_details_final;
create temp table customer_subscription_details_final as
with a as (
SELECT
    s.customer_id,
    count(case when s.subscriptions_per_customer=1 and c.payment_count=1
    and s.status = 'CANCELLED' then s.customer_id end) as fast_churn,
    count( s.subscription_id) AS subscriptions,
    min(s.start_date) AS start_date_of_first_subscription,
    max(s.start_date) AS start_date_of_last_subscription,
    max(s.cancellation_date) AS max_cancellation_date,
    sum(c.payment_count) as payment_count,
    sum(c.paid_subscriptions) as paid_subscriptions,
    sum(c.refunded_subscriptions) as refunded_subscriptions,
    sum(c.failed_subscriptions) as failed_subscriptions,
    sum(c.chargeback_subscriptions) as chargeback_subscriptions,
    count(
        CASE
            WHEN s.status::text = 'ACTIVE'::text
             THEN s.subscription_id
            ELSE NULL::character varying
        END) AS active_subscriptions,
       listagg(distinct(
        CASE
            WHEN s.status::text = 'ACTIVE'::text
             THEN s.product_name
            ELSE NULL::character varying
        END),' | ') AS active_subscription_product_names,
        listagg(distinct (
        CASE
            WHEN s.status::text = 'ACTIVE'::text
             THEN s.subcategory_name
            ELSE NULL::character varying
        END),' | ') AS active_subscription_subcategory,
   listagg(distinct (
        CASE
            WHEN s.status::text = 'ACTIVE'::text
             THEN s.category_name
            ELSE NULL::character varying
        END),' | ') AS active_subscription_category,
   listagg(distinct (
        CASE
            WHEN s.status::text = 'ACTIVE'::text
             THEN s.brand
            ELSE NULL::character varying
        END),' | ') AS active_subscription_brand,
       sum(
        CASE
            WHEN s.status::text = 'ACTIVE'::text
             THEN COALESCE(s.subscription_value, 0::double precision)
            ELSE 0::double precision
        END) AS active_subscription_value,
    count(
        CASE
            WHEN a.outstanding_assets > 0 THEN s.subscription_id
            ELSE NULL::character varying
        END) AS outstanding_subscriptions,
    sum(a.outstanding_assets) AS outstanding_assets,
  	count(
        CASE
            WHEN cr.cancellation_reason_new = 'DEBT COLLECTION' THEN cr.subscription_id
            ELSE NULL::character varying
        END) AS debt_collection_subscriptions,
            sum(c.total_cashflow) as total_cashflow,
    sum(s.committed_sub_value) as committed_subscription_value,
 sum(c.subscription_revenue_due) as subscription_revenue_due,
 sum(c.subscription_revenue_paid) as subscription_revenue_paid,
 sum(c.subscription_revenue_refunded) as subscription_revenue_refunded,
 sum(c.subscription_revenue_chargeback) as subscription_revenue_chargeback,
    listagg(distinct s.subscription_plan,' , ') within group (order by s.start_date DESC) AS subscription_durations,
    listagg(distinct
        CASE
            WHEN s.cancellation_date IS NULL THEN s.subscription_plan
            ELSE NULL
        END,' | ') within group (order by s.start_date DESC) AS active_subscription_durations,
    listagg(DISTINCT
        CASE
            WHEN s.rank_subscriptions = 1 THEN s.subscription_plan
            ELSE NULL
        END, ' | ') AS first_subscription_duration,
       listagg(DISTINCT
        CASE
            WHEN s.rank_subscriptions = 2 THEN s.subscription_plan
            ELSE NULL
        END, ' | ') AS second_subscription_duration,
            listagg(DISTINCT
        CASE
            WHEN s.rank_subscriptions = 1
			THEN s.store_label
            ELSE NULL
        END, ' | ') AS first_subscription_store,
                    listagg(DISTINCT
        CASE
            WHEN s.rank_subscriptions = 1
                             THEN s.store_short
            ELSE NULL
        END, ' | ') AS first_subscription_acquisition_channel,
       listagg(DISTINCT
        CASE
            WHEN s.rank_subscriptions = 2 THEN s.store_label
            ELSE NULL
        END, ' | ') AS second_subscription_store,
                    listagg(DISTINCT
        CASE
            WHEN s.rank_subscriptions = 1 THEN s.category_name
            ELSE NULL
        END, ' | ') AS first_subscription_product_category,
        listagg(DISTINCT
        CASE
            WHEN s.rank_subscriptions = 2 THEN s.category_name
            ELSE NULL
        END, ' | ') AS second_subscription_product_category,
        listagg(DISTINCT
                S.PRODUCT_NAME, ' , ') within group (order by s.start_date DESC)  AS ever_rented_products,
        listagg(DISTINCT
                S.product_sku, ' , ') within group (order by s.start_date DESC)  AS ever_rented_sku,
        listagg(DISTINCT
                S.variant_sku, ' , ') within group (order by s.start_date DESC)  AS ever_rented_variant_sku,
        listagg(DISTINCT
                S.brand, ' , ') within group (order by s.start_date DESC) AS ever_rented_brands,
        listagg(DISTINCT
                S.subcategory_name, ' , ') within group (order by s.start_date DESC) AS ever_rented_subcategories,
        listagg(DISTINCT
                S.category_name, ' , ') within group (order by s.start_date DESC) AS ever_rented_categories,
--count(distinct s.subcategory_name) as unique_subcategories,
--count(distinct s.category_name) as unique_categories,
count(case when s.category_name = 'Wearables' then s.subscription_id end) as subs_wearables,
count(case when s.category_name = 'Drones' then s.subscription_id end) as subs_drones,
count(case when s.category_name = 'Cameras' then s.subscription_id end) as subs_Cameras,
count(case when s.category_name = 'Phones & Tablets' then s.subscription_id end) as subs_Phones_and_tablets,
count(case when s.category_name = 'Computers' then s.subscription_id end) as subs_Computers,
count(case when s.category_name = 'Gaming & VR' then s.subscription_id end) as subs_gaming,
count(case when s.category_name = 'Audio & Music' then s.subscription_id end) as subs_audio,
count(case when s.category_name in ('POS','Home Entertainment','eMobility','Smart Home')
       or s.category_name is null then s.subscription_id end) as subs_other,
min(
  case
   when s.status = 'ACTIVE'
    and s.minimum_cancellation_date >= current_date
    then s.minimum_cancellation_Date
  end) as minimum_cancellation_Date,
count(case when s.subscription_plan = 'Pay As You Go' then s.subscription_id end) as subs_pag,
count(case when s.subscription_plan = '1 Months' then s.subscription_id end) as subs_1m,
count(case when s.subscription_plan = '3 Months' then s.subscription_id end) as subs_3m,
count(case when s.subscription_plan = '6 Months' then s.subscription_id end) as subs_6m,
count(case when s.subscription_plan = '12 Months' then s.subscription_id end) as subs_12m,
count(case when s.subscription_plan = '24 Months' then s.subscription_id end) as subs_24m,
sum(a.avg_asset_purchase_price) as ever_rented_asset_purchase_price,
case when sum(case
		when c.last_valid_payment_category like ('DEFAULT%')
			and a.outstanding_assets>0
			and c.dpd>30
			and c.dpd>(CURRENT_DATE - c.max_created_payment_due_date)
			and c.paid_subscriptions<5 then 1 else 0 end)>=1 then true else false end as is_bad_customer,
max(greatest(s.updated_date,a.updated_at,c.updated_at)) as updated_at
   FROM ODS_PRODUCTION.subscription s
     LEFT JOIN ods_production.subscription_assets a ON s.subscription_id::text = a.subscription_id::text
     LEFT JOIN ods_production.subscription_cashflow c ON s.subscription_id::text = c.subscription_id::text
     left join ods_production.subscription_cancellation_reason cr on cr.subscription_id = s.subscription_id
     group by 1
)
, sub_order as (
	select subscription_id,org.retention_group,o.order_id,start_date,cancellation_date,rank_subscriptions,order_rank,
	max(start_date) over (partition by s.order_id) as min_order_start,
	coalesce((max(cancellation_date) over (partition by s.order_id)),CURRENT_DATE) as max_ordeR_cancel,
	s.customer_id
	 from ods_production.subscription s
	left join ods_production."order" o on s.order_id = o.order_id
	left join ods_production.order_retention_group org on org.order_id = o.order_id
)
, b as (
	select
	a2.order_id,
	a2.max_order_cancel,
	a2.order_rank,
	max(case when a2.order_rank > a1.order_rank then a1.max_order_cancel end) as prev_order_cancellation
	from sub_order a1
	left join sub_order a2 on a1.customer_id = a2.customer_id
	group by 1,2,3)
, active_time as (
	select a.*,
	prev_order_cancellation,
    case when a.retention_group like '%NEW%' then datediff('d',a.start_date::timestamp,coalesce(a.max_order_cancel,CURRENT_DATE)::timestamp)
	when a.retention_group like '%REACTIVATION%' then datediff('d',a.min_order_start::timestamp,a.max_order_cancel::timestamp)
	when a.retention_group like '%UPSELL%' and (prev_order_cancellation >= a.max_order_cancel) then 0
	when  a.retention_group like '%UPSELL%' and (prev_order_cancellation < a.max_order_cancel) then (datediff('d',prev_order_cancellation::timestamp,coalesce(a.max_order_cancel,CURRENT_DATE)::timestamp))
	end as active_period from
	 sub_order a
	left join b on a.order_id = b.order_id
	order by rank_subscriptions ASC)
,active_time_grouped as (
	select customer_id,order_id,active_period
		from active_time c
	group by 1,2,3)
,customer_active_time as (
  Select customer_id, sum(active_period) as customer_active_time
  from active_time_grouped
  group by 1
  )
select a.*, product_names as minimum_cancellation_product,
customer_active_time
from a
left join (
select
 customer_id,
 minimum_cancellation_Date,
 listagg(distinct product_name,', ') as product_names
from ods_production.subscription s
group by 1,2) pro
 on pro.customer_id=a.customer_id
 and pro.minimum_cancellation_Date=a.minimum_cancellation_Date
left join customer_active_time ca
 on ca.customer_id = a.customer_id;

delete from ods_production.customer_subscription_details where 1=1;

insert into ods_production.customer_subscription_details
select * from customer_subscription_details_final;

--grant select on ods_production.customer_subscription_details to group risk_app_access;
