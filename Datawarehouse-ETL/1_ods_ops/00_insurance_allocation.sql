drop table if exists ods_production.insurance_allocation;

create table ods_production.insurance_allocation as 
WITH a AS (
         SELECT coalesce(i.asset__c,a.asset_id,asset.asset_Id) AS asset_id,
            coalesce(i.allocation__c,a.allocation_id) AS asset_allocation_id,
            allocation_sf_id,
            createddate as created_at,
            case 
            when company__c = 'COYA' 
             OR (contract__c LIKE 'CY%' AND company__c = 'Liventy') 
            then 'COYA'
            else 'ASSURANT' 
            end as insurance_partner,
            i.start__c AS contract_start_date,
            i.end__c AS CONTRACT_end_date,
            s.start_date as subscription_start_Date,
            a.created_at as allocation_created_date,
            a.return_delivery_date,
            a.refurbishment_start_at,
            s.cancellation_date as subscription_cancellation_date,
            sc.cancellation_reason_new as subscription_cancellation_reason,
            i.premium_rate__c AS premium_rate,
            i.premium_total__c AS premium_total,
            i.coverage__c AS coverage,
            i.months__c AS months_covered,
/*                CASE
                    WHEN asset_insurance__c.start__c >= '2018-01-01'::date OR asset_insurance__c.start__c < '2018-01-01'::date AND (asset_insurance__c.end__c IS NULL OR asset_insurance__c.end__c > '2018-01-01'::date) THEN 'keep'::text
                    ELSE 'exclude'::text
                END AS cohort_2018,*/
            i.contract__c AS contract_id,
 case when asset.subcategory_name in ('Smartphones','Smartwatches','Apple Watches','Tablets')
  then 'Category_Group_1'
  else 'Category_Group_2'
 end as category_group,
 case 
  when initial_price<=100
   and category_group = 'Category_Group_1'
    then 1.3
  when initial_price<=250
   and category_group = 'Category_Group_1'
    then 1.76
    when initial_price<=500
   and category_group = 'Category_Group_1'
    then 2.11
     when initial_price<=750
   and category_group = 'Category_Group_1'
    then 2.44
     when initial_price<=1000
   and category_group = 'Category_Group_1'
    then 2.76
      when initial_price<=1500
   and category_group = 'Category_Group_1'
    then 3.86
    when initial_price>1500
   and category_group = 'Category_Group_1'
    then 3.86
    --
     when coalesce(initial_price,0)<=100
   and category_group = 'Category_Group_2'
    then 0.97
  when initial_price<=250
   and category_group = 'Category_Group_2'
    then 1.28
    when initial_price<=500
   and category_group = 'Category_Group_2'
    then 1.64
     when initial_price<=750
   and category_group = 'Category_Group_2'
    then 2.03
     when initial_price<=1000
   and category_group = 'Category_Group_2'
    then 2.48
      when initial_price<=1500
   and category_group = 'Category_Group_2'
    then 3.22
         when initial_price<=2000
   and category_group = 'Category_Group_2'
    then 4.16
        when initial_price<=3000
   and category_group = 'Category_Group_2'
    then 5.32
      when initial_price<=4000
   and category_group = 'Category_Group_2'
    then 6.76
  when initial_price<=5000
   and category_group = 'Category_Group_2'
    then 8.52
    when initial_price>5000
   and category_group = 'Category_Group_2'
    then 8.52
   else null
  end as monthly_premium_contract,
  case when initial_price>=1000 then 'mandatory'
   else 'voluntary'
   end as insurance_condition,
   asset.subcategory_name,
   asset.initial_price as purchase_price
           FROM stg_salesforce.asset_insurance__c i 
           left join ods_production.allocation a 
            on i.allocation__c=a.allocation_id
           left join ods_production.asset asset
            on asset.asset_id=a.asset_id
           left join ods_production.subscription s 
            on s.subscription_id=a.subscription_id
            left join ods_production.subscription_cancellation_reason sc 
            on sc.subscription_id=a.subscription_id
           where premium_rate__c is not null
        ), b AS (
         SELECT a.asset_id,
            a.asset_allocation_id,
            a.allocation_sf_id,
            a.insurance_partner,
            a.contract_id,
            CASE WHEN CONTRACT_END_DATE IS NOT NULL 
            THEN 'CANCELLED'
            WHEN contract_end_date IS NULL 
            AND coalesce(REFURBISHMENT_START_AT,subscription_cancellation_date) IS NULL 
            THEN 'ACTIVE'
            WHEN contract_end_date IS NULL 
            AND coalesce(REFURBISHMENT_START_AT,subscription_cancellation_date) IS NOT NULL 
            THEN 'ACTIVE BUT ASSET RETURNED'
            END AS CONTRACT_STATUS,
            a.created_at,
            a.contract_start_date,
            a.contract_end_date,
            a.subscription_start_Date,
            a.allocation_created_date,
            a.return_delivery_date,
            a.refurbishment_start_at,
            a.subscription_cancellation_date,
            a.subscription_cancellation_reason,
            greatest(round(DATEDIFF('day',contract_start_date::timestamp,coalesce(REFURBISHMENT_START_AT,subscription_cancellation_date,current_date)::timestamp)/30),1) as months_on_rent,
            a.premium_rate,
            a.premium_total,
            a.coverage,
            a.months_covered,
            monthly_premium_contract,
            insurance_condition,
            purchase_price,
            subcategory_name
/*            a.cohort_2018,
                CASE
                    WHEN a.cohort_2018 = 'keep'::text AND §_date < '2018-01-01'::date AND a.end_date IS NULL THEN ceiling((date_trunc('month'::text, 'now'::text::date::timestamp with time zone)::date - '2018-01-01'::date)::numeric / 30::numeric)::double precision
                    WHEN a.cohort_2018 = 'keep'::text AND a.start_date < '2018-01-01'::date AND a.end_date IS NOT NULL THEN ceiling((date_trunc('month'::text, a.end_date::timestamp with time zone)::date - '2018-01-01'::date)::numeric / 30::numeric)::double precision
                    WHEN a.cohort_2018 = 'keep'::text AND a.start_date >= '2018-01-01'::date THEN a.months_covered
                    WHEN a.start_date < '2018-01-01'::date AND a.end_date < '2018-01-01'::date THEN 0::double precision
                    ELSE NULL::double precision
                END AS months_to_offset,
                CASE
                    WHEN a.cohort_2018 = 'keep'::text AND a.start_date < '2018-01-01'::date AND a.end_date IS NULL THEN ceiling((date_trunc('month'::text, 'now'::text::date::timestamp with time zone)::date - '2018-01-01'::date)::numeric / 30::numeric)::double precision * a.premium_rate
                    WHEN a.cohort_2018 = 'keep'::text AND a.start_date < '2018-01-01'::date AND a.end_date IS NOT NULL THEN ceiling((date_trunc('month'::text, a.end_date::timestamp with time zone)::date - '2018-01-01'::date)::numeric / 30::numeric)::double precision * a.premium_rate
                    WHEN a.cohort_2018 = 'keep'::text AND a.start_date >= '2018-01-01'::date THEN a.premium_total
                    WHEN a.start_date < '2018-01-01'::date AND a.end_date < '2018-01-01'::date THEN 0::double precision
                    ELSE NULL::double precision
                END AS premium_2018*/
           FROM a
          ORDER BY a.subscription_start_date DESC
        )
        ,c as(
 SELECT 
    b.asset_id,
    b.asset_allocation_id,
    b.allocation_sf_id,
    b.insurance_partner,
    b.contract_id,
    b.CONTRACT_STATUS,
    b.created_at,
    b.contract_start_date,
    b.contract_end_date,
    lead(contract_start_date) over (partition by asset_allocation_id order by contract_start_date) as next_contract_start_date,
    lead(contract_end_date) over (partition by asset_allocation_id order by contract_start_date) as next_contract_end_date,
    lead(contract_id) over (partition by asset_allocation_id order by contract_start_date) as next_contract_id,
    (lead(contract_start_date) over (partition by asset_allocation_id order by contract_start_date))::date-  coalesce(b.contract_end_date::date,current_date) as overlapping_days,
    coalesce(REFURBISHMENT_START_AT,subscription_cancellation_date) as asset_returned_date,
    coalesce(b.subscription_cancellation_reason,'ACTIVE') as subscription_cancellation_reason,
    b.months_on_rent,
    b.premium_rate,
    b.premium_total,
    b.coverage,
    b.months_covered,
    monthly_premium_contract as monthly_premium_coya,
    insurance_condition,
    purchase_price,
    subcategory_name,
    CASE WHEN CONTRACT_STATUS NOT IN ('ACTIVE') THEN GREATEST(b.months_covered-b.months_on_rent,0) ELSE NULL END AS months_overpayment,
    CASE WHEN CONTRACT_STATUS NOT IN ('ACTIVE') THEN GREATEST(b.months_covered-b.months_on_rent,0)*b.premium_rate ELSE NULL END as premium_overpayment,
    count(contract_id) over (partition by asset_allocation_id) as contracts_per_allocation,
    count(case when contract_end_date is null then contract_id end) over (partition by asset_allocation_id) as open_contracts_per_allocation,
    rank() over (partition by asset_allocation_id order by contract_start_date,created_at) as rank_contracts
   FROM b)
   select 
   *
   from c;

GRANT SELECT ON ods_production.insurance_allocation TO tableau;
