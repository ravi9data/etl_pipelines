drop table if exists ods_production.customer;
create table ods_production.customer as 
WITH web AS (
  SELECT
    DISTINCT o.user_id AS customer_id,
    max(o.bill_address_id) AS billing_address_id,
    max(o.ship_address_id) AS shipping_address_id
  FROM stg_api_production.spree_orders o
  WHERE
    o.user_id IS NOT NULL
    AND (
      o.bill_address_id IS NOT NULL
      OR o.ship_address_id IS NOT NULL
    )
  GROUP BY
    o.user_id
),
a AS (
  SELECT
    spree_addresses.user_id,
    max(spree_addresses.created_at) AS max_created
  FROM stg_api_production.spree_addresses
  GROUP BY
    spree_addresses.user_id
),
b AS (
  SELECT
    ad.user_id,
    ad.city,
    ad.zipcode,
    ad.address1 AS street,
    ad.address2 AS house_number
  FROM a
  JOIN stg_api_production.spree_addresses ad ON a.user_id = ad.user_id
    AND ad.created_at = a.max_created
),
address AS (
  SELECT
    a.id,
    a.firstname,
    a.lastname,
    c_1.name AS country_iso_name,
    c_1.iso AS country_code,
    NULLIF(a.city :: text, '' :: text) AS city,
    NULLIF(a.zipcode :: text, '' :: text) AS zipcode,
    NULLIF(a.address1 :: text, '' :: text) AS street,
    NULLIF(a.address2 :: text, '' :: text) AS house_number,
    NULLIF(a.additional_info :: text, '' :: text) AS additional_info,
    NULLIF(a.company :: text, '' :: text) AS company,
    a.phone,
    a.created_at AS source_create_timestamp,
    GREATEST(a.created_at, a.updated_at) AS source_update_timestamp,
    greatest(a.updated_at, c_1.updated_at) as updated_at
  FROM stg_api_production.spree_addresses a
  JOIN stg_api_production.spree_countries c_1 ON c_1.id = a.country_id
)
, dim_states as (
select 
*,
plz::text as plz,
length((plz::text)) as length_,
trim(case when length((plz::text))=4 then ('0'+plz::text) else plz::text end) as plz_new
from public.dim_states)
select
  distinct u.id as customer_id,
  a.id as customer_sf_id,
  c.company_id,
  u.created_at,
  max(greatest(u.updated_at,a.lastmodifieddate,a.systemmodstamp,c.updated_at, a1.updated_at, a2.updated_At)) over (partition by u.id) as updated_at,
  case when coalesce(g.gender,lower(u.gender),  max( case when coalesce(g.gender,lower(u.gender)) in ('male','m') then 'm'
  when coalesce(g.gender,lower(u.gender)) in ('female','f', 'w') then 'f' 
  else coalesce(g.gender,lower(u.gender)) end) over (partition by lower(u.first_name))) in ('male','m') then 'm' 
  when coalesce(g.gender,lower(u.gender),  max( case when coalesce(g.gender,lower(u.gender)) in ('male','m') then 'm' 
  when coalesce(g.gender,lower(u.gender)) in ('female','f', 'w') then 'f' 
  else coalesce(g.gender,lower(u.gender)) end) over (partition by lower(u.first_name)))='female' then 'f' 
  else coalesce(g.gender,lower(u.gender),  max( case when coalesce(g.gender,lower(u.gender)) in ('male','m') then 'm' 
  when coalesce(g.gender,lower(u.gender)) in ('female','f', 'w') then 'f' 
  else coalesce(g.gender,lower(u.gender)) end) over (partition by lower(u.first_name))) end as gender,
  "left"('now' :: text :: date :: character varying :: text, 4) :: numeric - "left"(u.birthdate :: character varying :: text, 4) :: numeric AS age,
  u.subscription_limit,
  u.subscription_limit_change_date,
  COALESCE(u.user_type, 'normal_customer') AS customer_type,
  c.company_name as company_name,
	c.status AS company_status,
	c.company_type_id,
	c.company_type_name,
	c.created_at as company_created_at,
  -- u.subscribed_to_newsletter,
  a1.country_iso_name AS billing_country,
  a2.country_iso_name AS shipping_country,
  TRIM(LOWER(a1.city)) AS billing_city,
  a1.zipcode AS billing_zip,
  --a2.country_iso_name AS shipping_country,
  COALESCE(TRIM(LOWER(a2.city)), TRIM(LOWER(b.city))) :: character varying(128) AS shipping_city,
  COALESCE(a2.zipcode, b.zipcode) :: character varying(16) AS shipping_zip,
  u.signup_language,
  u.default_locale,
  s.bundesland,
  case
    when u.referral_code not like ('G-%') then 'G-' || u.referral_code
    else u.referral_code
  end as referral_code,
  case
    when u.mailchimp_status = 'unsubscribed' then 'Unsubscribed'
    when u.mailchimp_status = 'subscribed'
    and u.confirmed_at is null then 'Subscribed'
    when u.mailchimp_status = 'subscribed'
    and u.confirmed_at is not null then 'Opted In'
  end as email_subscribe,
  case  
  	WHEN  COALESCE(u.user_type, 'normal_customer')='business_customer' and c.status='pending_documents' THEN 'pending'
    when u.phone_number_verified_at is not null 
     and u.confirmed_at is not null
      then 'complete'
      else 'incomplete'
    end as profile_status,
    cd.consolidation_day,
    cd.updatedat as consolidation_updated_at
from stg_api_production.spree_users u
left join stg_external_apis.name_gender g 
 on g.firstname= SPLIT_PART(lower(u.first_name),' ',1)
left join stg_salesforce.v_account a on a.spree_customer_id__c = u.id
LEFT JOIN web w ON u.id = w.customer_id
LEFT JOIN ods_production.companies c on u.company_id = c.company_id
LEFT JOIN b ON b.user_id = u.id
LEFT JOIN address a1 ON a1.id = COALESCE(w.billing_address_id, u.bill_address_id)
LEFT JOIN address a2 ON a2.id = COALESCE(w.shipping_address_id, u.ship_address_id)
left join ods_b2b.consolidation_date cd on cd.customer_id = u.id
left join (
    select
      distinct plz_new as plz,
      max(bundesland) as bundesland
    from dim_states
    group by
      1
  ) s on COALESCE(a2.zipcode, b.zipcode) :: character varying(16) = s.plz :: character varying(16)
    where u.id is not null
  ;
 --removing deleted customers
GRANT SELECT ON ods_production.customer TO tableau;
GRANT SELECT ON ods_production.customer TO b2b_redash;
