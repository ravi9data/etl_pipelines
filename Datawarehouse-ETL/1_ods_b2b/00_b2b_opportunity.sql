drop table if exists ods_b2b.opportunity;
create table ods_b2b.opportunity as
		select 
id as opportunity_id
,accountid as account_id
,contactid as contact_id
,company_id__c as company_id
,name 
,description
,stagename as stage_name
,amount
,probability
,closedate as closed_date
,type as opportunity_type
,nextstep as next_step
,leadsource as lead_source
,isclosed as is_closed
,iswon as is_won
,forecastcategory as forecast_cateory
,forecastcategoryname as forecast_category_name
,ownerid as owner_id
,createddate as created_date
,createdbyid as created_id
,lastmodifieddate
,lastmodifiedbyid
,systemmodstamp
,lastactivitydate
,fiscalquarter as fiscal_quarter
,fiscalyear as fiscal_year
,fiscal
,lastvieweddate
,lastreferenceddate
,hasopenactivity as has_open_activity
,hasoverduetask as has_overdue_task
,lastamountchangedhistoryid
,lastclosedatechangedhistoryid
,budget_confirmed__c
,discovery_completed__c
,roi_analysis_completed__c
,loss_reason__c as loss_reason
,rentalperiod__c
,usecasedetails__c
,discounts__c
,discountreason__c
,discountamount__c
,partnershipdescription__c
,creditlimit__c
,first_opp_name__c as first_opp_name
,automated__c as is_automated
,admin_panel_link__c
,legacy_deal_owner__c as legacy_owner
,migrated__c as is_migrated
,rental_period__c
,csv__c
,weighted_asv__c
,assist_field_conversion_rate__c
from stg_sfb2b.opportunity
;