with dates as (
select 
    date_trunc('month',datum) as months 
    from public.dim_dates dd
    where datum<=current_date and datum>='2021-03-01')----as all the first customer is from 2015
,customers as (
select
    distinct
    date as bom,
    from master.subscription_historical sh 
    left join public.dim_dates dd 
    on dd.datum = sh.date 
    where (dd.day_is_first_of_month is true or date = current_date) and 
     date >= '2021-03-01'
    and status = 'ACTIVE'
    and country_name='Germany'
    and customer_type='normal_customer'
    group by 1 order by 1 desc)
,Card_users AS (
SELECT 
    date_trunc('month',first_card_created_date) AS card_created_month , 
    count(distinct case when active_card_first_event='created' then customer_id end) as total_card_users,
    count(DISTINCT CASE WHEN first_activated_date IS NOT  NULL THEN customer_id  END ) AS active_cards
    WHERE first_card_created_date IS NOT NULL
    GROUP BY 1)
,views as (
select  
    date_trunc('month',page_view_start) as page_view_start
    ,count(distinct session_id) as session_traffic
    ,count(distinct anonymous_id) as user_traffic
    FROM traffic.page_views pv 
    and page_view_start>='2021-09-01'
    group by 1
    )
,waitlist as (
select 
    date_trunc('month',created_at) as waiting_list_date
    ,count(distinct customer_id) as users_in_waitlist
    group by 1)
,onboarding as(
select 
    date_trunc('month',card_request_date) as card_request_date
    ,count(distinct customer_id) as card_requested_users
    group by 1)
, card as (
select 
    date_trunc('month',user_created_date) as card_month
    ,count(distinct customer_id) as completed_personal_information
    ,count(distinct case when status_final_identification='pending' then customer_id end ) as identification_pending
    ,count(distinct case when status_final_identification='aborted' then customer_id end ) as identification_aborted
    ,count(distinct case when status_final_identification='successful' then customer_id end ) as completed_identification
    ,sum(amount_transaction_money_received) as amount_transaction_money_received
    ,sum(amount_transaction_payment_successful) as amount_transaction_payment_successful
    ,count(distinct case when is_active_card=true then customer_id end) as Active_card_users
    group by 1)
,new_subs AS (
    SELECT 
        date_trunc('month',start_date)::Date AS sub_month, 
        count(DISTINCT subscription_id) as Total_subscriptions,
        count(case when start_date<First_card_created_date  then subscription_id end ) as  total_subscription_before_card,
        count( CASE  WHEN First_card_created_date < start_date THEN subscription_id END ) AS Total_card_subscriptions,
        --count(DISTINCT CASE  WHEN is_after_card=1 THEN subscription_id END ) AS Total_card_subscriptions,
        sum(subscription_value) as Total_subscription_value,
        sum(case when First_card_created_date < start_date  THEN  subscription_value END ) as Total_card_subscription_value,
        count(case when status ='ACTIVE'  then subscription_id end) as Active_subscription,
        count(DISTINCT case WHEN First_card_created_date < start_date and status ='ACTIVE' then subscription_id END ) as Active_card_subscriptions,
        --count(DISTINCT case WHEN First_card_created_date< start_date and status ='ACTIVE' then subscription_id END ) as Active_card_subscriptions_new,
        sum(case when status ='ACTIVE'  then subscription_value end) as Active_subscription_value,
        sum(case when is_after_card =1 and status ='ACTIVE' then subscription_value end) as Active_card_subscription_value,
        sum(case when is_after_card =1 and status ='ACTIVE'  then committed_sub_value end) as active_committed_card_subscription_value,
        sum(case when is_after_card =1  then committed_sub_value end) as  acquired_committed_card_subscription_value,
        count(case when datediff('day',CAST(First_card_created_date AS timestamp),CAST(start_date AS timestamp)) between -15 and 15 and status ='ACTIVE'  then subscription_id end) as card_active_subs_15days,
        sum(case when datediff('day',CAST(First_card_created_date AS timestamp) ,CAST(start_date AS timestamp)) between -15 and 15 and status ='ACTIVE'  then subscription_value end) as card_Active_subvalue_15days
    WHERE customer_type='normal_customer'
    GROUP BY 1
    ORDER BY 1 desc
)
,active_subs_before AS (
    SELECT 
        date_trunc('month',first_card_created_date) AS card_month ,
        sum(aso.active_subscriptions) AS  active_subscriptions_before_card,
        sum(aso.active_subscription_value) AS active_subscription_value_before_card,
        sum(aso.active_committed_subscription_value) as active_committed_subscription_value_before_card
    FROM (SELECT DISTINCT customer_id,
        first_value(first_event_timestamp) over (partition by customer_id order by first_event_timestamp asc
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_card_created_date 
    INNER JOIN ods_finance.active_subscriptions_overview aso 
    on aso.customer_id=gc2.customer_id
    AND first_card_created_date::Date=aso.fact_date
    GROUP BY 1
        )
,cash_pre AS (
    SELECT 
        date_trunc('month', event_timestamp)::date AS redeem_date,
        WHERE user_classification='Card_user'
        GROUP BY 1
)
,cash AS (
    SELECT 
        redeem_date,
        sum(total_cash_issuance) OVER(ORDER BY redeem_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT row  ) AS running_issuance,
        sum(total_cash_redemptions) OVER(ORDER BY redeem_date ROWS BETWEEN UNBOUNDED PRECEDING AND current row) AS running_redemption
    FROM cash_pre
)
,transactions as (
select 
        date_trunc('month',event_timestamp) as transaction_date
        ,count(case when rn=1 and event_name='money-received' then customer_id end ) as first_deposit
        ,count(case when rn=1 and event_name='payment-successful' then customer_id end ) as first_purchase
        ,count(distinct case when transaction_type='PURCHASE' and event_name='payment-successful' then customer_id end) as unique_transaction
        ,sum(case when transaction_type='PURCHASE' and event_name='payment-successful' then amount_transaction end) as unique_transaction_amount
        ,count(case when transaction_type='PURCHASE' and event_name='payment-successful' then customer_id end) as Total_transactions
        from (select customer_id,event_name,transaction_type,event_timestamp,amount_transaction,
        row_number() over (partition by customer_id,event_name order by event_timestamp) rn
        group by 1)
,payments AS (
SELECT 
        date_trunc('month',due_date) AS payment_created_month
        ,count(DISTINCT CASE WHEN payment_type ='FIRST' AND status ='PAID' THEN subscription_payment_id END ) AS first_card_payment
        ,count(DISTINCT CASE WHEN payment_type='RECURRENT' AND status ='PAID' THEN subscription_payment_id END ) AS reccurent_card_payment
        ,count(DISTINCT CASE WHEN payment_type='RECURRENT' AND status ='FAILED' AND paid_date IS null THEN subscription_payment_id END ) AS Failed_card_payment
        ,sum(CASE WHEN payment_type='RECURRENT' AND status ='FAILED' AND paid_date IS null THEN amount_subscription END ) AS Subscription_amount_failed
        ,count(DISTINCT CASE WHEN payment_type='RECURRENT' AND paid_date IS NOT NULL AND failed_date IS NOT NULL AND failed_date<paid_date THEN subscription_id END ) AS missed_card_first_attempt
        WHERE first_card_created_date<due_Date
        GROUP BY 1
        ORDER BY 1 desc
    )
,
account_closure AS 
(
    SELECT 
        date_trunc('month',legal_closure_date) AS account_closure_date_,
        count(distinct customer_id) AS closed_accounts
    GROUP BY 1)
select 
    distinct d.months::Date
    ,coalesce(cu.total_card_users,0) as total_card_users
    ,coalesce(cd.Active_card_users,0) as Active_card_users
    ,COALESCE(ac.closed_accounts,0) AS closed_accounts 
    ,coalesce(cu.active_cards,0) as active_cards
    ,coalesce(v.session_traffic,0) as session_traffic
    ,coalesce(v.user_traffic,0) as user_traffic
    ,coalesce(w.users_in_waitlist,0) as users_in_waitlist
    ,coalesce(o.card_requested_users,0) as card_requested_users
    ,coalesce(cd.completed_personal_information,0) as completed_personal_information
    ,coalesce(cd.completed_personal_information-cd.identification_pending-cd.identification_aborted-cd.completed_identification,0) as Identification_Outstanding
    ,coalesce(cd.identification_pending,0) as identification_pending
    ,coalesce(cd.identification_aborted,0) as identification_aborted
    ,coalesce(completed_identification,0) as completed_identification
    ,coalesce(Round((cu.total_card_users::decimal/o.card_requested_users::decimal),2),0) as onboarding_completion
    ,coalesce(cd.amount_transaction_money_received,0) as amount_transaction_money_received
    ,coalesce(cd.amount_transaction_payment_successful,0) as amount_transaction_payment_successful
    ,coalesce(t.first_deposit,0) as first_deposit
    ,coalesce(t.first_purchase,0) as first_purchase
    ,coalesce(t.unique_transaction,0) as unique_transaction
    ,coalesce(t.unique_transaction_amount,0) as unique_transaction_amount
    ,coalesce(t.Total_transactions,0) as Total_transactions
    ,coalesce(c.Grover_cash,0) as Grover_cash
    ,coalesce(c.running_issuance,0) as running_issuance
    ,coalesce(c.running_redemption,0) as running_redemption
    ,coalesce(s.Total_subscriptions,0) as Total_subscriptions
    ,coalesce(s.Total_card_subscriptions,0) as Total_card_subscriptions
    ,coalesce(asb.active_subscriptions_before_card,0) as active_subscriptions_before_card
    ,coalesce(s.Active_subscription,0) as Active_subscription
    ,coalesce(s.Active_card_subscriptions,0) as Active_card_subscriptions
    ,coalesce(s.total_subscription_value,0) as total_subscription_value
    ,coalesce(s.Total_card_subscription_value,0) as Total_card_subscription_value
    ,coalesce(s.Active_subscription_value,0) as Active_subscription_value
    ,coalesce(asb.active_subscription_value_before_card,0) as active_subscription_value_before_card
    ,coalesce(s.Active_card_subscription_value,0) as Active_card_subscription_value
    ,coalesce(asb.active_committed_subscription_value_before_card,0) as active_committed_subscription_value_before_card
    ,coalesce(s.active_committed_card_subscription_value,0) as active_committed_card_subscription_value
    ,coalesce(s.card_active_subs_15days,0) as card_active_subs_15days
    ,coalesce(s.card_Active_subvalue_15days,0) as card_Active_subvalue_15days
    ,coalesce(first_card_payment,0) as first_card_payment
    ,coalesce(reccurent_card_payment,0) as reccurent_card_payment
    ,coalesce(Failed_card_payment,0) as Failed_card_payment
    ,coalesce(Subscription_amount_failed,0) as Subscription_amount_failed
    ,coalesce(missed_card_first_attempt,0) as missed_card_first_attempt
from dates d
left join customers cust
    on d.months=cust.bom
left join views v 
    on d.months=v.page_view_start
left join waitlist w
    on d.months=w.waiting_list_date
left join onboarding o
    on d.months=o.card_request_date
left join card cd
    on d.months=cd.card_month
left join new_subs s
    on d.months=s.sub_month
left join cash c
    on d.months=c.redeem_date
left join transactions t
    on d.months=t.transaction_date
LEFT JOIN payments ps 
    ON ps.payment_created_month=d.months
LEFT JOIN card_users cu 
    ON cu.card_created_month=d.months
LEFT JOIN active_subs_before asb 
    ON asb.card_month=d.months
LEFT JOIN account_closure ac 
    ON d.months=ac.account_closure_date_
order by 1 DESC;


----dwh Weekly table

with dates as (
select 
    date_trunc('week',datum::date) as weeks 
    from public.dim_dates dd
    where datum<=current_date and datum>='2021-03-01')----as all the first customer is from 2015
,customers as (
select
    distinct
    date::date as bow,
    from master.subscription_historical sh 
    left join public.dim_dates dd 
    on dd.datum = sh.date 
    where --(dd.day_is_first_of_month is true or date = current_date) and 
     date >= '2021-03-01'
    and status = 'ACTIVE'
    and country_name='Germany'
    and customer_type='normal_customer'
    group by 1 order by 1 desc)
,Card_users AS (
SELECT 
    date_trunc('week',first_card_created_date) AS card_created_week , 
    count(distinct case when active_card_first_event='created' then customer_id end) as total_card_users,
    count(DISTINCT CASE WHEN first_activated_date IS NOT  NULL THEN customer_id  END ) AS active_cards
    WHERE first_card_created_date IS NOT NULL
    GROUP BY 1)
,views as (
select  
    date_trunc('week',page_view_start) as page_view_start
    ,count(distinct session_id) as session_traffic
    ,count(distinct anonymous_id) as user_traffic
    FROM traffic.page_views pv 
    and page_view_start>='2021-09-01'
    group by 1
    )
,waitlist as (
select 
    date_trunc('week',created_at) as waiting_list_date
    ,count(distinct customer_id) as users_in_waitlist
    group by 1)
,onboarding as(
select 
    date_trunc('week',card_request_date) as card_request_date
    ,count(distinct customer_id) as card_requested_users
    group by 1)
, card as (
select 
    date_trunc('week',user_created_date) as card_week
    ,count(distinct customer_id) as completed_personal_information
    ,count(distinct case when status_final_identification='pending' then customer_id end ) as identification_pending
    ,count(distinct case when status_final_identification='aborted' then customer_id end ) as identification_aborted
    ,count(distinct case when status_final_identification='successful' then customer_id end ) as completed_identification
    ,sum(amount_transaction_money_received) as amount_transaction_money_received
    ,sum(amount_transaction_payment_successful) as amount_transaction_payment_successful
    ,count(distinct case when is_active_card=true then customer_id end) as Active_card_users
    group by 1)
,new_subs AS (
    SELECT 
        date_trunc('week',start_date)::Date AS sub_week, 
        count(DISTINCT subscription_id) as Total_subscriptions,
        count(case when start_date<First_card_created_date  then subscription_id end ) as  total_subscription_before_card,
        count( CASE  WHEN First_card_created_date < start_date THEN subscription_id END ) AS Total_card_subscriptions,
        --count(DISTINCT CASE  WHEN is_after_card=1 THEN subscription_id END ) AS Total_card_subscriptions,
        sum(subscription_value) as Total_subscription_value,
        sum(case when First_card_created_date < start_date  THEN  subscription_value END ) as Total_card_subscription_value,
        count(case when status ='ACTIVE'  then subscription_id end) as Active_subscription,
        count(DISTINCT case WHEN First_card_created_date < start_date and status ='ACTIVE' then subscription_id END ) as Active_card_subscriptions,
        --count(DISTINCT case WHEN First_card_created_date< start_date and status ='ACTIVE' then subscription_id END ) as Active_card_subscriptions_new,
        sum(case when status ='ACTIVE'  then subscription_value end) as Active_subscription_value,
        sum(case when is_after_card =1 and status ='ACTIVE' then subscription_value end) as Active_card_subscription_value,
        sum(case when is_after_card =1 and status ='ACTIVE'  then committed_sub_value end) as active_committed_card_subscription_value,
        sum(case when is_after_card =1  then committed_sub_value end) as  acquired_committed_card_subscription_value,
        count(case when datediff('day',CAST(First_card_created_date AS timestamp),CAST(start_date AS timestamp)) between -15 and 15 and status ='ACTIVE'  then subscription_id end) as card_active_subs_15days,
        sum(case when datediff('day',CAST(First_card_created_date AS timestamp) ,CAST(start_date AS timestamp)) between -15 and 15 and status ='ACTIVE'  then subscription_value end) as card_Active_subvalue_15days
    WHERE customer_type='normal_customer'
    GROUP BY 1
    ORDER BY 1 desc
)
,active_subs_before AS (
    SELECT 
        date_trunc('week',first_card_created_date) AS card_week ,
        sum(aso.active_subscriptions) AS  active_subscriptions_before_card,
        sum(aso.active_subscription_value) AS active_subscription_value_before_card,
        sum(aso.active_committed_subscription_value) as active_committed_subscription_value_before_card
    FROM (SELECT DISTINCT customer_id,
        first_value(first_event_timestamp) over (partition by customer_id order by first_event_timestamp asc
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_card_created_date 
    INNER JOIN ods_finance.active_subscriptions_overview aso 
    on aso.customer_id=gc2.customer_id
    AND first_card_created_date::Date=aso.fact_date
    GROUP BY 1
        )
,cash_pre AS (
    SELECT 
        date_trunc('week', event_timestamp)::date AS redeem_date,
        WHERE user_classification='Card_user'
        GROUP BY 1
)
,cash AS (
    SELECT 
        redeem_date,
        sum(total_cash_issuance) OVER(ORDER BY redeem_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT row  ) AS running_issuance,
        sum(total_cash_redemptions) OVER(ORDER BY redeem_date ROWS BETWEEN UNBOUNDED PRECEDING AND current row) AS running_redemption
    FROM cash_pre
)
,transactions as (
select 
        date_trunc('week',event_timestamp) as transaction_date
        ,count(case when rn=1 and event_name='money-received' then customer_id end ) as first_deposit
        ,count(case when rn=1 and event_name='payment-successful' then customer_id end ) as first_purchase
        ,count(distinct case when transaction_type='PURCHASE' and event_name='payment-successful' then customer_id end) as unique_transaction
        ,sum(case when transaction_type='PURCHASE' and event_name='payment-successful' then amount_transaction end) as unique_transaction_amount
        ,count(case when transaction_type='PURCHASE' and event_name='payment-successful' then customer_id end) as Total_transactions
        from (select customer_id,event_name,transaction_type,event_timestamp,amount_transaction,
        row_number() over (partition by customer_id,event_name order by event_timestamp) rn
        group by 1)
,payments AS (
SELECT 
        date_trunc('week',due_date) AS payment_created_week
        ,count(DISTINCT CASE WHEN payment_type ='FIRST' AND status ='PAID' THEN subscription_payment_id END ) AS first_card_payment
        ,count(DISTINCT CASE WHEN payment_type='RECURRENT' AND status ='PAID' THEN subscription_payment_id END ) AS reccurent_card_payment
        ,count(DISTINCT CASE WHEN payment_type='RECURRENT' AND status ='FAILED' AND paid_date IS null THEN subscription_payment_id END ) AS Failed_card_payment
        ,sum(CASE WHEN payment_type='RECURRENT' AND status ='FAILED' AND paid_date IS null THEN amount_subscription END ) AS Subscription_amount_failed
        ,count(DISTINCT CASE WHEN payment_type='RECURRENT' AND paid_date IS NOT NULL AND failed_date IS NOT NULL AND failed_date<paid_date THEN subscription_id END ) AS missed_card_first_attempt
        WHERE first_card_created_date<due_Date
        GROUP BY 1
        ORDER BY 1 desc
    )
/*,max_out AS (
SELECT 
        date_trunc('week',ps2.due_date)AS payment_created_week
        ,count(CASE WHEN ps2.amount_paid-ps2.amount_discount<1 THEN ps2.subscription_payment_id end) AS Maxed_out_payments
        ,sum(CASE WHEN ps2.amount_paid-ps2.amount_discount<1 THEN ps2.amount_paid END) AS amount_max
        ,count(DISTINCT CASE WHEN ps2.amount_discount <0 THEN ps2.subscription_id end) AS subscription_with_redemption
    FROM 
        WHERE ps2.due_date >first_card_created_date 
        GROUP BY 1 )*/
,
account_closure AS 
(
    SELECT 
        date_trunc('week',legal_closure_date) AS account_closure_date_,
        count(distinct customer_id) AS closed_accounts
    GROUP BY 1)
select 
    distinct d.weeks::Date
    ,coalesce(cu.total_card_users,0) as total_card_users
    ,coalesce(cd.Active_card_users,0) as Active_card_users
    ,COALESCE(ac.closed_accounts,0) AS closed_accounts 
    ,coalesce(cu.active_cards,0) as active_cards
    ,coalesce(v.session_traffic,0) as session_traffic
    ,coalesce(v.user_traffic,0) as user_traffic
    ,coalesce(w.users_in_waitlist,0) as users_in_waitlist
    ,coalesce(o.card_requested_users,0) as card_requested_users
    ,coalesce(cd.completed_personal_information,0) as completed_personal_information
    ,coalesce(cd.completed_personal_information-cd.identification_pending-cd.identification_aborted-cd.completed_identification,0) as Identification_Outstanding
    ,coalesce(cd.identification_pending,0) as identification_pending
    ,coalesce(cd.identification_aborted,0) as identification_aborted
    ,coalesce(completed_identification,0) as completed_identification
    ,coalesce(Round((cu.total_card_users::decimal/o.card_requested_users::decimal),2),0) as onboarding_completion
    ,coalesce(cd.amount_transaction_money_received,0) as amount_transaction_money_received
    ,coalesce(cd.amount_transaction_payment_successful,0) as amount_transaction_payment_successful
    ,coalesce(t.first_deposit,0) as first_deposit
    ,coalesce(t.first_purchase,0) as first_purchase
    ,coalesce(t.unique_transaction,0) as unique_transaction
    ,coalesce(t.unique_transaction_amount,0) as unique_transaction_amount
    ,coalesce(t.Total_transactions,0) as Total_transactions
    ,coalesce(c.Grover_cash,0) as Grover_cash
    ,coalesce(c.running_issuance,0) as running_issuance
    ,coalesce(c.running_redemption,0) as running_redemption
    ,coalesce(s.Total_subscriptions,0) as Total_subscriptions
    ,coalesce(s.Total_card_subscriptions,0) as Total_card_subscriptions
    ,coalesce(asb.active_subscriptions_before_card,0) as active_subscriptions_before_card
    ,coalesce(s.Active_subscription,0) as Active_subscription
    ,coalesce(s.Active_card_subscriptions,0) as Active_card_subscriptions
    ,coalesce(s.total_subscription_value,0) as total_subscription_value
    ,coalesce(s.Total_card_subscription_value,0) as Total_card_subscription_value
    ,coalesce(s.Active_subscription_value,0) as Active_subscription_value
    ,coalesce(asb.active_subscription_value_before_card,0) as active_subscription_value_before_card
    ,coalesce(s.Active_card_subscription_value,0) as Active_card_subscription_value
    ,coalesce(asb.active_committed_subscription_value_before_card,0) as active_committed_subscription_value_before_card
    ,coalesce(s.active_committed_card_subscription_value,0) as active_committed_card_subscription_value
    ,coalesce(s.card_active_subs_15days,0) as card_active_subs_15days
    ,coalesce(s.card_Active_subvalue_15days,0) as card_Active_subvalue_15days
    ,coalesce(first_card_payment,0) as first_card_payment
    ,coalesce(reccurent_card_payment,0) as reccurent_card_payment
    ,coalesce(Failed_card_payment,0) as Failed_card_payment
    ,coalesce(Subscription_amount_failed,0) as Subscription_amount_failed
    ,coalesce(missed_card_first_attempt,0) as missed_card_first_attempt
from dates d
left join customers cust
    on d.weeks=cust.bow
left join views v 
    on d.weeks=v.page_view_start
left join waitlist w
    on d.weeks=w.waiting_list_date
left join onboarding o
    on d.weeks=o.card_request_date
left join card cd
    on d.weeks=cd.card_week
left join new_subs s
    on d.weeks=s.sub_week
left join cash c
    on d.weeks=c.redeem_date
left join transactions t
    on d.weeks=t.transaction_date
LEFT JOIN payments ps 
    ON ps.payment_created_week=d.weeks
LEFT JOIN card_users cu 
    ON cu.card_created_week=d.weeks
LEFT JOIN active_subs_before asb 
    ON asb.card_week=d.weeks
LEFT JOIN account_closure ac 
    ON d.weeks=ac.account_closure_date_
order by 1 DESC
;

--Card Onboarding daily 
with dates as (
select 
    date_trunc('day',datum) as day_ 
    from public.dim_dates dd
    where datum<=current_date and datum>='2021-03-01')----as all the first customer is from 2015
,customers as (
select
    distinct
    date as bom,
    from master.subscription_historical sh 
    left join public.dim_dates dd 
    on dd.datum = sh.date 
    where (dd.day_is_first_of_month is true or date = current_date) and 
     date >= '2021-03-01'
    and status = 'ACTIVE'
    and country_name='Germany'
    and customer_type='normal_customer'
    group by 1 order by 1 desc)
,Card_users AS (
SELECT 
    date_trunc('day',first_card_created_date) AS card_created_day , 
    count(distinct case when active_card_first_event='created' then customer_id end) as total_card_users,
    count(DISTINCT CASE WHEN first_activated_date IS NOT  NULL THEN customer_id  END ) AS active_cards
    WHERE first_card_created_date IS NOT NULL
    GROUP BY 1)
,views as (
select  
    date_trunc('day',page_view_start) as page_view_start
    ,count(distinct session_id) as session_traffic
    ,count(distinct anonymous_id) as user_traffic
    FROM traffic.page_views pv 
    and page_view_start>='2021-09-01'
    group by 1
    )
,waitlist as (
select 
    date_trunc('day',created_at) as waiting_list_date
    ,count(distinct customer_id) as users_in_waitlist
    group by 1)
,onboarding as(
select 
    date_trunc('day',card_request_date) as card_request_date
    ,count(distinct customer_id) as card_requested_users
    group by 1)
, card as (
select 
    date_trunc('day',user_created_date) as card_day
    ,count(distinct customer_id) as completed_personal_information
    ,count(distinct case when status_final_identification='pending' then customer_id end ) as identification_pending
    ,count(distinct case when status_final_identification='aborted' then customer_id end ) as identification_aborted
    ,count(distinct case when status_final_identification='successful' then customer_id end ) as completed_identification
    ,sum(amount_transaction_money_received) as amount_transaction_money_received
    ,sum(amount_transaction_payment_successful) as amount_transaction_payment_successful
    ,count(distinct case when is_active_card=true then customer_id end) as Active_card_users
    group by 1)
,new_subs AS (
    SELECT 
        date_trunc('day',start_date)::Date AS sub_day, 
        count(DISTINCT subscription_id) as Total_subscriptions,
        count(case when start_date<First_card_created_date  then subscription_id end ) as  total_subscription_before_card,
        count( CASE  WHEN First_card_created_date < start_date THEN subscription_id END ) AS Total_card_subscriptions,
        --count(DISTINCT CASE  WHEN is_after_card=1 THEN subscription_id END ) AS Total_card_subscriptions,
        sum(subscription_value) as Total_subscription_value,
        sum(case when First_card_created_date < start_date  THEN  subscription_value END ) as Total_card_subscription_value,
        count(case when status ='ACTIVE'  then subscription_id end) as Active_subscription,
        count(DISTINCT case WHEN First_card_created_date < start_date and status ='ACTIVE' then subscription_id END ) as Active_card_subscriptions,
        --count(DISTINCT case WHEN First_card_created_date< start_date and status ='ACTIVE' then subscription_id END ) as Active_card_subscriptions_new,
        sum(case when status ='ACTIVE'  then subscription_value end) as Active_subscription_value,
        sum(case when First_card_created_date < start_date AND first_card_created_date IS NOT NULL and status ='ACTIVE' then subscription_value end) as Active_card_subscription_value,
        sum(case when First_card_created_date < start_date  and status ='ACTIVE'  then committed_sub_value end) as active_committed_card_subscription_value,
        sum(case when First_card_created_date < start_date  then committed_sub_value end) as  Total_committed_card_subscription_value,
        count(case when datediff('day',CAST(First_card_created_date AS timestamp),CAST(start_date AS timestamp)) between -15 and 15 and status ='ACTIVE'  then subscription_id end) as card_active_subs_15days,
        sum(case when datediff('day',CAST(First_card_created_date AS timestamp) ,CAST(start_date AS timestamp)) between -15 and 15 and status ='ACTIVE'  then subscription_value end) as card_Active_subvalue_15days
    WHERE customer_type='normal_customer'
    GROUP BY 1
    ORDER BY 1 desc
)
,active_subs_before AS (
SELECT 
        date_trunc('day',fact_date) AS card_day ,
        count(DISTINCT gc2.customer_id) AS customers,
        sum(CASE WHEN first_card_created_date::Date=aso.fact_date then aso.active_subscriptions END ) AS  active_subscriptions_before_card,
        sum(CASE WHEN first_card_created_date::Date=aso.fact_date THEN aso.active_subscription_value END ) AS active_subscription_value_before_card,
        sum(CASE WHEN first_card_created_date::Date=aso.fact_date then aso.active_committed_subscription_value END ) as active_committed_subscription_value_before_card,
        sum(CASE WHEN (current_date-1)::Date=aso.fact_date then aso.active_subscriptions END ) AS  active_subscriptions_now,
        sum(CASE WHEN (current_date-1)::Date=aso.fact_date THEN aso.active_subscription_value END ) AS active_subscription_value_now,
        sum(CASE WHEN (current_date-1)::Date=aso.fact_date then aso.active_committed_subscription_value END ) as active_committed_subscription_value_now,
        active_subscriptions_now-active_subscriptions_before_card AS incremental_active_subscriptions,
       	active_subscription_value_now-active_subscription_value_before_card AS incremental_active_subscriptions_value,
        active_committed_subscription_value_now-active_committed_subscription_value_before_card AS incremental_active_committed_subscriptions_value
    FROM (SELECT DISTINCT customer_id,
        first_value(first_event_timestamp) over (partition by customer_id order by first_event_timestamp asc
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_card_created_date 
    INNER JOIN ods_finance.active_subscriptions_overview aso 
    on aso.customer_id=gc2.customer_id
    GROUP BY 1
        )
,cash_pre AS (
    SELECT 
        date_trunc('day', event_timestamp)::date AS redeem_date,
        WHERE user_classification='Card_user'
        GROUP BY 1
)
,cash AS (
    SELECT 
        redeem_date,
        sum(total_cash_issuance) OVER(ORDER BY redeem_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT row  ) AS running_issuance,
        sum(total_cash_redemptions) OVER(ORDER BY redeem_date ROWS BETWEEN UNBOUNDED PRECEDING AND current row) AS running_redemption
    FROM cash_pre
)
,transactions as (
select 
        date_trunc('day',event_timestamp) as transaction_date
        ,count(case when rn=1 and event_name='money-received' then customer_id end ) as first_deposit
        ,count(case when rn=1 and event_name='payment-successful' then customer_id end ) as first_purchase
        ,count(distinct case when transaction_type='PURCHASE' and event_name='payment-successful' then customer_id end) as unique_transaction
        ,sum(case when transaction_type='PURCHASE' and event_name='payment-successful' then amount_transaction end) as unique_transaction_amount
        ,count(case when transaction_type='PURCHASE' and event_name='payment-successful' then customer_id end) as Total_transactions
        from (select customer_id,event_name,transaction_type,event_timestamp,amount_transaction,
        row_number() over (partition by customer_id,event_name order by event_timestamp) rn
        group by 1)
,payments AS (
SELECT 
        date_trunc('day',due_date) AS payment_created_day
        ,count(DISTINCT CASE WHEN payment_type ='FIRST' AND status ='PAID' THEN subscription_payment_id END ) AS first_card_payment
        ,count(DISTINCT CASE WHEN payment_type='RECURRENT' AND status ='PAID' THEN subscription_payment_id END ) AS reccurent_card_payment
        ,count(DISTINCT CASE WHEN payment_type='RECURRENT' AND status ='FAILED' AND paid_date IS null THEN subscription_payment_id END ) AS Failed_card_payment
        ,sum(CASE WHEN payment_type='RECURRENT' AND status ='FAILED' AND paid_date IS null THEN amount_subscription END ) AS Subscription_amount_failed
        ,count(DISTINCT CASE WHEN payment_type='RECURRENT' AND paid_date IS NOT NULL AND failed_date IS NOT NULL AND failed_date<paid_date THEN subscription_id END ) AS missed_card_first_attempt
        WHERE first_card_created_date<due_Date
        GROUP BY 1
        ORDER BY 1 desc
    )
,
account_closure AS 
(
    SELECT 
        date_trunc('day',legal_closure_date) AS account_closure_date_,
        count(distinct customer_id) AS closed_accounts
    GROUP BY 1)
,mau AS
(
	SELECT
		datum AS mau_day,
			(SELECT 
				COUNT(DISTINCT tt.customer_id)
			WHERE transaction_type  = 'PURCHASE'
				AND event_name  = 'payment-successful' 
				AND tt.event_timestamp BETWEEN DATE_ADD('day', -23, mau_day) AND DATE_ADD('day', 6, mau_day)) AS MAU_moving30day -- 30 days IN total (average per yr is 30.43)
	FROM public.dim_dates dd
		WHERE TRUE --AND week_day_number = 1
		AND datum BETWEEN '2021-04-01' and current_date
	)
, CS AS 
	(
      SELECT 
        date_trunc('day',cr.first_card_assigned)::date AS first_assigned_date
        ,count(DISTINCT id) AS total_conversations
        ,count(CASE WHEN is_closed_by_bot=TRUE THEN id END ) AS totaL_conversation_closed_by_bot
      WHERE cr.is_card_assigned=True
      GROUP BY 1
	)
select 
    distinct d.day_::Date
    ,coalesce(cu.total_card_users,0) as total_card_users
    ,coalesce(cd.Active_card_users,0) as Active_card_users
    ,COALESCE(ac.closed_accounts,0) AS closed_accounts 
    ,coalesce(cu.active_cards,0) as active_cards
    ,coalesce(v.session_traffic,0) as session_traffic
    ,coalesce(v.user_traffic,0) as user_traffic
    ,coalesce(w.users_in_waitlist,0) as users_in_waitlist
    ,coalesce(o.card_requested_users,0) as card_requested_users
    ,coalesce(cd.completed_personal_information,0) as completed_personal_information
    ,coalesce(cd.completed_personal_information-cd.identification_pending-cd.identification_aborted-cd.completed_identification,0) as Identification_Outstanding
    ,coalesce(cd.identification_pending,0) as identification_pending
    ,coalesce(cd.identification_aborted,0) as identification_aborted
    ,coalesce(completed_identification,0) as completed_identification
    ,coalesce(Round((cu.total_card_users::decimal/o.card_requested_users::decimal),2),0) as onboarding_completion
    ,coalesce(MAU_moving30day,0) as MAU_moving30day
    --transactions
    ,coalesce(cd.amount_transaction_money_received,0) as amount_transaction_money_received
    ,coalesce(cd.amount_transaction_payment_successful,0) as amount_transaction_payment_successful
    ,coalesce(t.first_deposit,0) as first_deposit
    ,coalesce(t.first_purchase,0) as first_purchase
    ,coalesce(t.unique_transaction,0) as unique_transaction
    ,coalesce(t.unique_transaction_amount,0) as unique_transaction_amount
    ,coalesce(t.Total_transactions,0) as Total_transactions
    ,coalesce(c.Grover_cash,0) as Grover_cash
    ,coalesce(c.running_issuance,0) as running_issuance
    ,coalesce(c.running_redemption,0) as running_redemption
    --total subscriptions
    ,coalesce(s.Total_subscriptions,0) as Total_subscriptions
    ,coalesce(s.Total_card_subscriptions,0) as Total_card_subscriptions
    --Active subscriptions
    ,coalesce(asb.active_subscriptions_before_card,0) as active_subscriptions_before_card
    ,coalesce(s.Active_subscription,0) as Active_subscription
    ,coalesce(s.Active_card_subscriptions,0) as Active_card_subscriptions
    ,coalesce(asb.active_subscriptions_now,0) as active_subscriptions_now
    ---total Subscription value
    ,coalesce(s.total_subscription_value,0) as total_subscription_value
    ,coalesce(s.Total_card_subscription_value,0) as Total_card_subscription_value
    --Active subscription value
    ,coalesce(s.Active_subscription_value,0) as Active_subscription_value
    ,coalesce(asb.active_subscription_value_before_card,0) as active_subscription_value_before_card
    ,coalesce(s.Active_card_subscription_value,0) as Active_card_subscription_value
    ,coalesce(asb.active_subscription_value_now,0) as active_subscription_value_now
    --committed_subscription_value
    ,coalesce(asb.active_committed_subscription_value_before_card,0) as active_committed_subscription_value_before_card
    ,coalesce(s.active_committed_card_subscription_value,0) as active_committed_card_subscription_value
    ,coalesce(s.Total_committed_card_subscription_value,0) as Total_committed_card_subscription_value
    ,coalesce(asb.active_committed_subscription_value_now,0) as active_committed_subscription_value_now
   	---incremental info 
    ,coalesce(asb.incremental_active_subscriptions,0) as incremental_active_subscriptions
    ,coalesce(asb.incremental_active_subscriptions_value,0) as incremental_active_subscriptions_value
    ,coalesce(asb.incremental_active_committed_subscriptions_value,0) as incremental_active_committed_subscriptions_value
    --Payments
    ,coalesce(s.card_active_subs_15days,0) as card_active_subs_15days
    ,coalesce(s.card_Active_subvalue_15days,0) as card_Active_subvalue_15days
    ,coalesce(first_card_payment,0) as first_card_payment
    ,coalesce(reccurent_card_payment,0) as reccurent_card_payment
    ,coalesce(Failed_card_payment,0) as Failed_card_payment
    ,coalesce(Subscription_amount_failed,0) as Subscription_amount_failed
    ,coalesce(missed_card_first_attempt,0) as missed_card_first_attempt
    --Customer Service
    ,coalesce(Total_conversations,0) as total_conversations
    ,coalesce(totaL_conversation_closed_by_bot,0) as totaL_conversation_closed_by_bot
from dates d
left join customers cust
    on d.day_=cust.bom
left join views v 
    on d.day_=v.page_view_start
left join waitlist w
    on d.day_=w.waiting_list_date
left join onboarding o
    on d.day_=o.card_request_date
left join card cd
    on d.day_=cd.card_day
left join new_subs s
    on d.day_=s.sub_day
left join cash c
    on d.day_=c.redeem_date
left join transactions t
    on d.day_=t.transaction_date
LEFT JOIN payments ps 
    ON ps.payment_created_day=d.day_
LEFT JOIN card_users cu 
    ON cu.card_created_day=d.day_
LEFT JOIN active_subs_before asb 
    ON asb.card_day=d.day_
LEFT JOIN account_closure ac 
    ON d.day_=ac.account_closure_date_
LEFT JOIN mau m 
    ON d.day_=m.mau_day
LEFT JOIN CS cs 
	ON cs.first_assigned_date=d.day_
order by 1 DESC
WITH NO SCHEMA binding;

