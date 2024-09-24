
drop table if exists dm_risk.mng_investor_request_asset_portfolio;
create table dm_risk.mng_investor_request_asset_portfolio as
with asset_cohort AS 
(
SELECT
	ah.date as d_bom,
	ah.asset_id ,
	ah.customer_id ,
	ah.asset_status_original ,
	ah.active_subscription_value ,
	ah.initial_price as purchase_price,
	ah.residual_value_market_price ,
	ah.active_subscriptions_bom ,
	ah.capital_source_name ,
	ah.asset_condition ,
	ah.brand ,
	ah.category_name ,
	ah.subcategory_name ,
	ah.office_or_sponsorships ,
	ah.country ,
	ah.warehouse 
FROM
master.asset_historical ah 
where ah."date" = date_trunc('month',date)
and customer_id  is not null
order by date desc
),
customer_data_pre as(
select 
distinct
date_trunc('month',ch."date") as date ,
customer_id ,
customer_type,
ch.schufa_class,
ch.burgel_risk_category,
ch.burgel_score
from
master.customer_historical ch 
),
all_customers as
(
select 
distinct
date ,
customer_id ,
customer_type,
last_value(ch.schufa_class) over (partition by customer_id, date order by date rows between unbounded preceding and unbounded following ) as schufa_class ,
last_value(ch.burgel_risk_category) over (partition by customer_id, date  order by date rows between unbounded preceding and unbounded following ) as burgel_risk_category ,
last_value(ch.burgel_score) over (partition by customer_id, date order by date rows between unbounded preceding and unbounded following ) as burgel_score 
from
customer_data_pre ch 
where customer_type = 'normal_customer'
),
final_pre as (
SELECT 
	a.*,
	c.customer_type,
	c.schufa_class,
	c.burgel_risk_category,
	c.burgel_score
FROM 
asset_cohort a
left join
all_customers c
on a.customer_id = c.customer_id
and a.d_bom = c.date
where c.customer_type = 'normal_customer'
),
tmp_order_schufa as(
select 
updated_at ::date as updated_at,
customer_id ,
score_range as schufa_class
from s3_spectrum_rds_dwh_order_approval.schufa_data
),
final_1 as(
select
	distinct portfolio.*,
	last_value(order_.schufa_class) over (partition by order_.customer_id order by order_.updated_at rows between unbounded preceding and unbounded following ) last_
from
	final_pre portfolio
left join tmp_order_schufa order_ on
	portfolio.customer_id = order_.customer_id
	and date_trunc('month', order_.updated_at::date) <= portfolio.d_bom
),
tmp_order_burgel as(
select 
updated_at ::date as updated_at,
customer_id ,
cast(score_value as float) as burgel_score
--select distinct 
from s3_spectrum_rds_dwh_order_approval.crifburgel_data
where country_code = 'DE'
),
final_ as(
select
	distinct portfolio.*,
	last_value(order_.burgel_score) over (partition by order_.customer_id order by order_.updated_at rows between unbounded preceding and unbounded following ) last_burgel
from
	final_1 portfolio
left join tmp_order_burgel order_ on
	portfolio.customer_id = order_.customer_id
	and date_trunc('month', order_.updated_at::date) <= portfolio.d_bom
),
at_crif_score as
(
select 
distinct
a.customer_id ,
date_trunc('month', a.updated_at ::date) as order_submitted_month,
score_value::decimal(38,2) as at_crif_score_value,
CASE WHEN at_crif_score_value BETWEEN 0 AND 419 THEN '5 (0-419)'
			 WHEN at_crif_score_value BETWEEN 420 AND 449 THEN '4 (420-449)'
			 WHEN at_crif_score_value BETWEEN 450 AND 509 THEN '3 (450-509)'
			 WHEN at_crif_score_value BETWEEN 510 AND 549 THEN '2 (510-549)'
			 WHEN at_crif_score_value BETWEEN 550 AND 623 THEN '1 (550-623)'
		ELSE 'N/A'
		END AS at_crif_rating
from 
s3_spectrum_rds_dwh_order_approval.crifburgel_data a
where score_value is not null
and country_code = 'AT'
),
nl_focum_rating as
(
select 
distinct
a.customer_id ,
date_trunc('month', a.updated_at ::date)::date as order_submitted_month,
CASE WHEN score_value::decimal BETWEEN -90 AND 590 THEN 'NL10 (-90-590)'
			 WHEN score_value::decimal BETWEEN 591 AND 703 THEN 'NL09 (591-703)'
			 WHEN score_value::decimal BETWEEN 704 AND 783 THEN 'NL08 (704-783)'
			 WHEN score_value::decimal BETWEEN 784 AND 848 THEN 'NL07 (784-848)'
			 WHEN score_value::decimal BETWEEN 849 AND 898 THEN 'NL06 (849-889)'
			 WHEN score_value::decimal BETWEEN 899 AND 937 THEN 'NL05 (899-937)'
			 WHEN score_value::decimal BETWEEN 938 AND 953 THEN 'NL04 (938-953)'
			 WHEN score_value::decimal BETWEEN 954 AND 968 THEN 'NL03 (954-968)'
			 WHEN score_value::decimal BETWEEN 969 AND 981 THEN 'NL02 (969-981)'
			 WHEN score_value::decimal BETWEEN 982 AND 9999 THEN 'NL01 (982-999)'
		ELSE 'N/A'
		END AS nl_focum_rating
from 
s3_spectrum_rds_dwh_order_approval.experian_data a
),
es_equifax_rating as
(
select 
distinct
a.customer_id ,
date_trunc('month', a.updated_at ::date)::date as order_submitted_month,
CASE WHEN  score_value::decimal BETWEEN -9999 AND 6 THEN 'ES08 (-9999-6)'
			 WHEN  score_value::decimal BETWEEN 7 AND 29 THEN 'ES07 (7-29)'
			 WHEN  score_value::decimal BETWEEN 30 AND 292 THEN 'ES06 (30-292)'
			 WHEN  score_value::decimal BETWEEN 293 AND 520 THEN 'ES05 (293-520)'
			 WHEN  score_value::decimal BETWEEN 521 AND 628 THEN 'ES04 (521-628)'
			 WHEN  score_value::decimal BETWEEN 629 AND 779 THEN 'ES03 (629-779)'
			 WHEN  score_value::decimal BETWEEN 780 AND 827 THEN 'ES02 (780-827)'
			 WHEN  score_value::decimal BETWEEN 828 AND 9999 THEN 'ES01 (828-999)'
		ELSE 'N/A'
		END AS es_equifax_rating
from 
s3_spectrum_rds_dwh_order_approval.equifax_risk_score_data a
),
es_experian as
(
select 
distinct
a.customer_id ,
date_trunc('month', a.updated_at ::date)::date as order_submitted_month,
score_note as es_experian_rating
from 
s3_spectrum_rds_dwh_order_approval.experian_es_delphi_data a
)
SELECT 
distinct
f.*,
COALESCE(schufa_class, last_) as schufa_class_final,
COALESCE(burgel_score, last_burgel) as burgel_score_final,
case 
	when schufa_class_final in ('M','N','O','P') then 'Schufa Class (M-N-O-P)'
	when schufa_class_final is null or schufa_class = '' then 'Schufa Class NULL/Blanks'
	when schufa_class_final in ('A','B','C','D','E','F','G','H','I','J','K','L') then 'Schufa Class (A to L)'
	else schufa_class_final
end as schufa_class_grouped,
case
	when burgel_score_final < 2 then 'Burgel LT2'
	when burgel_score_final < 3 then 'Burgel LT3'
	when burgel_score_final < 4 then 'Burgel LT4'
	when burgel_score_final <=5 then 'Burgel LT5'
	when burgel_score_final > 5 then 'Burgel 5-6'
	else 'NULL/Blank'
end as burgel_score_grouped,
a.at_crif_rating,
n.nl_focum_rating,
es_equifax_rating,
es_experian_rating
from final_ f
	left join
	at_crif_score a
	on a.customer_id = f.customer_id
	and a.order_submitted_month <= f.d_bom
		left join
		nl_focum_rating n
		on n.customer_id = f.customer_id
		and n.order_submitted_month <= f.d_bom
			left join
			es_equifax_rating e
			on e.customer_id = f.customer_id
			and e.order_submitted_month <= f.d_bom
				left join
				es_experian ee
				on ee.customer_id = f.customer_id
				and ee.order_submitted_month <= f.d_bom
;

GRANT SELECT ON dm_risk.mng_investor_request_asset_portfolio TO tableau;
