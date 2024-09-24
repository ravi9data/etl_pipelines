drop table if exists ods_b2b.account;
create table ods_b2b.account as 
select 
id as account_id
,name as account_name
,type as account_type
,billingstreet
,billingcity
,billingstate
,billingpostalcode
,billingcountry
,shippingstreet
,shippingcity
,shippingstate
,shippingpostalcode
,shippingcountry
,phone
,website
,industry
,numberofemployees
,ownerid as account_owner
,createddate as created_date
,createdbyid as created_id
,lastmodifieddate as last_modified_date
,lastmodifiedbyid as last_modified_id
,systemmodstamp
,lastactivitydate
,lastvieweddate
,lastreferenceddate
,accountsource as account_source
,email__c as account_email
,creditlimit__c as credit_limit
,preferredpaymentmethod__c as payment_method
,admin_panel_link__c as admin_panel_link
,failedpaymentsinfo__c
,customerid__c as customer_id
,riskrecommendation__c
,personalreferral__c
,bocompanylink__c
,companyid__c as company_id
,preferreddevices__c
,leadsegment__c as lead_segement
,from_legacy_system__c
,sflink__c
,legacy_account_owner__c
,migrated__c as is_migrated
,employees__c
,year_of_foundation__c
,state__c as state 
,lead_segment_category__c as lead_segment_category
,su_subsegment__c as su_subsegment
,eb_subsegment__c as eb_subsegment
,public_institution__c as public_institution
,international__c as international
,key_account__c
,legal_entity_type_acc__c as legal_entity_type
,cs__c
,account_differentiation__c as account_differentiation
,country__c
,acc_w_o_order_with_act_w_open_opp__c
,accounts_w_o_order_w_o_open_opp__c
,assistfield_account__c
,of_open_activities__c
,linkedin_si__company_profile__c
,account_health__c
,assist_field_freelancer__c
from 
	stg_sfb2b.account
    where isdeleted = false;

GRANT SELECT ON ods_b2b.account TO tableau;
GRANT SELECT ON ods_b2b.account TO b2b_redash;
