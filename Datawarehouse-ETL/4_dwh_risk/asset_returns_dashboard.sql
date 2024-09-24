drop table if exists dm_risk.asset_returns;
create table dm_risk.asset_returns as
with subscription as
(
select 
*,
case when s.cancellation_reason_new in ('FAILED DELIVERY',
'CANCELLED BEFORE ALLOCATION - CUSTOMER REQUEST',
'LOST DURING OUTBOUND',
'CANCELLED BEFORE ALLOCATION - OTHERS',
'CANCELLED BEFORE SHIPMENT',
'CANCELLED BEFORE ALLOCATION - PROCUREMENT',
'SOLD EARLY','SOLD 1-EUR') then 1 else 0 end as cancellation_exclusions,
case 
	when s.dpd <=0 then '1. 0 DPD'
	when s.dpd between 1 and 30 then '2. DPD 01-30'
	when s.dpd between 31 and 60 then '3. DPD 31-60'
	when s.dpd between 61 and 90 then '4. DPD 61-90'
	when s.dpd > 90 then '5. DPD 90+'
	else 'NULL' 
end as dpd_bucket
FROM 
master.subscription s 
),
asset_is_returned as 
(
select
sp.subscription_id ,
max(case when sp.asset_was_returned is true then 1 else 0 end) as fl_asset_returned,
max(case when sp.asset_was_delivered is true then 1 else 0 end) as fl_asset_delivered,
max(case when sp.payment_number = 2 and sp.failed_date is not null then 1 else 0 end) as fl_fpd,
max(dpd) as max_dpd
from
master.subscription_payment sp 
group by 1
),
score_dist as 
(
select 
*
from ods_data_sensitive.external_provider_order_score 
),
ml_scores as
(
select 
case
	when d.score is null then 'NULL'
	when d.score <= 0.1 then '1-10'
	when d.score <= 0.2 then '11-20'
	when d.score <= 0.3 then '21-30'
	when d.score <= 0.4 then '31-40'
	when d.score <= 0.5 then '41-50'
	when d.score <= 0.6 then '51-60'
	when d.score <= 0.7 then '61-70'
	when d.score <= 0.8 then '71-80'
	when d.score <= 0.9 then '81-90'
	when d.score <= 1 then '91-100'
else 'Unknown'
end as ml_score_buckets,
order_id 
from 
ods_production.order_scoring d
)
select 
s.*,
r.fl_asset_returned,
r.fl_asset_delivered,
r.fl_fpd,
c.at_crif_score_rating,
c.at_crif_score_value,
c.es_experian_score_rating,
c.es_experian_score_value,
c.es_equifax_score_rating,
c.es_equifax_score_value,
c.de_schufa_score_rating,
c.de_schufa_score_value,
c.nl_focum_score_rating,
c.nl_focum_score_value,
c.nl_experian_score_rating,
c.nl_experian_score_value,
c.us_precise_score_rating,
c.us_precise_score_value,
c.us_fico_score_rating,
c.us_fico_score_value,
m.ml_score_buckets,
case 
when s.status = 'CANCELLED' and cancellation_exclusions = 0 and fl_asset_delivered = 1 and (fl_asset_returned = 0 or fl_asset_returned is null) and datediff('day',s.cancellation_date,current_date) > 10 then 'NOT RETURNED'
when s.status = 'CANCELLED' and cancellation_exclusions = 0 and fl_asset_delivered = 1 and (fl_asset_returned = 0 or fl_asset_returned is null) then 'NOT RETURNED'
when s.status = 'CANCELLED' and cancellation_exclusions = 0 and fl_asset_returned = 1 and datediff('day',s.cancellation_date,current_date) <= 10 then 'RETURNED'
when s.status = 'CANCELLED' and cancellation_exclusions = 0 and fl_asset_returned = 1 then 'RETURNED'
when s.status = 'CANCELLED' and cancellation_exclusions = 1 then 'CANCELLATION EXCLUSION'
when s.status = 'CANCELLED' and (fl_asset_delivered = 0 or fl_asset_delivered is null) then 'CANCELLATION EXCLUSION'
when s. status = 'ACTIVE' then 'ACTIVE'
else 'LEAK' end as return_status
FROM 
subscription s
left join
asset_is_returned r
on r.subscription_id = s.subscription_id
left join 
score_dist c
on c.order_id = s.order_id
left join 
ml_scores m
on m.order_id = s.order_id
;

GRANT SELECT ON dm_risk.asset_returns TO tableau;
