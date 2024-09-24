truncate table stg_order_approval.focum_data;
insert into stg_order_approval.focum_data
select
 created_at,
  updated_at,
  id,
  order_id,
  customer_id,
  address_id,
  address_key,
  risk_result,
  (risk_score::float)::int,
  risk_decision,
  risk_description,
  debt_result,
  debt_person,
  debt_address,
  debt_zipcode,
  hits_result,
  hits_count_person,
  hits_rating_person,
  hits_distinct_person,
  hits_count_address,
  hits_rating_address,
  hits_distinct_address,
  mtch_count_calls,
  mtch_distinct_calls,
  stat_pc4_count,
  stat_pc4_index,
  stat_pc5_count,
  stat_pc5_index,
  stat_pc6_count,
  stat_pc6_index,
  caps_person,
  raw_data,
  extracted_at,
CASE
WHEN (risk_score::float)::int <= 590 THEN 'NL10'
WHEN (risk_score::float)::int <= 703 THEN 'NL09'
WHEN (risk_score::float)::int<= 783 THEN 'NL08'
WHEN (risk_score::float)::int<= 848 THEN 'NL07'
WHEN (risk_score::float)::int <= 898 THEN 'NL06'
WHEN (risk_score::float)::int <= 937 THEN 'NL05'
WHEN (risk_score::float)::int<= 953 THEN 'NL04'
WHEN (risk_score::float)::int <= 968 THEN 'NL03'
WHEN (risk_score::float)::int<= 981 THEN 'NL02'
WHEN (risk_score::float)::int> 981 THEN 'NL01' end as focum_rating,
CASE
WHEN (risk_score::float)::int<= 590 THEN 0.1768
WHEN (risk_score::float)::int<= 703 THEN 0.1551
WHEN (risk_score::float)::int<= 783 THEN 0.0819
WHEN (risk_score::float)::int <= 848 THEN 0.0688
WHEN (risk_score::float)::int<= 898 THEN 0.0588
WHEN (risk_score::float)::int<= 937 THEN 0.055
WHEN (risk_score::float)::int<= 953 THEN 0.0439
WHEN (risk_score::float)::int<= 968 THEN 0.0346
WHEN (risk_score::float)::int<= 981 THEN 0.0333
WHEN (risk_score::float)::int> 981 THEN 0.0118 end as focum_pd,
row_number() over (partition by id order by updated_at desc) rn
from
   s3_spectrum_rds_dwh_order_approval.focum_data s;

truncate table stg_order_approval.equifax_risk_score_data;
insert into stg_order_approval.equifax_risk_score_data
select
	id,
	order_id,
	customer_id,
	transaction_id,
	transaction_state,
	interaction_id,
	raw_data,
	cast(score_value as float),
	case
		when cast(score_value as float) <= 6 then 'ES08'
		when cast(score_value as float) <= 29 then 'ES07'
		when cast(score_value as float) <= 292 then 'ES06'
		when cast(score_value as float) <= 520 then 'ES05'
		when cast(score_value as float) <= 628 then 'ES04'
		when cast(score_value as float) <= 779 then 'ES03'
		when cast(score_value as float) <= 827 then 'ES02'
		when cast(score_value as float) > 827 then 'ES01'
	end as equifax_rating,
	case
		when cast(score_value as float) <= 6 then 0.1792
		when cast(score_value as float) <= 29 then 0.1449
		when cast(score_value as float) <= 292 then 0.1123
		when cast(score_value as float) <= 520 then 0.0989
		when cast(score_value as float) <= 628 then 0.0827
		when cast(score_value as float) <= 779 then 0.0615
		when cast(score_value as float) <= 827 then 0.0462
		when cast(score_value as float) > 827 then 0.0339
	end as equifax_pd,
	agg_unpaid_products,
	extracted_at,
	row_number() over (partition by id
order by
	updated_at desc) rn
from
	s3_spectrum_rds_dwh_order_approval.equifax_risk_score_data s;
