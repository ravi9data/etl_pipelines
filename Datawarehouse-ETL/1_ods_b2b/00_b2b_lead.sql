drop table if exists ods_b2b.lead;
	create table ods_b2b.lead as 
		select
		id as lead_id
,lastname as last_name
,firstname as first_name
,salutation as salutation
,name as full_name
,title as lead_title
,company
,street
,city
,state
,postalcode
,country
,phone
,mobilephone
,email
,website
,leadsource
,status as lead_status 
,industry
,numberofemployees
,ownerid as lead_owner
,isconverted as is_converted
,converteddate as converted_date
,convertedaccountid as converted_account_id
,convertedcontactid as converted_contact_id
,convertedopportunityid as converted_opportunity_id
,isunreadbyowner
,createddate as created_date
,createdbyid
,lastmodifieddate
,lastmodifiedbyid
,systemmodstamp
,lastactivitydate
,lastvieweddate
,lastreferenceddate
,emailbouncedreason
,emailbounceddate
,noofemployees__c as employees
,legalentitytype__c as legal_type
,legal_representative_email__c
,bo_company_link__c
,legacy_lead_owner__c as legacy_owner
,salesforce_link__c as sf_link
,riskrecommendation__c as risk_recommendation
,migrated__c as is_migrated
,company_status__c
,email_2__c as secondary_email
,call_status__c as call_status
, company_lead_type__c as company_lead_type
,islegalrep__c as is_legal_rep
,new_lead_segment__c as new_lead_segment
,su_subsegment__c as su_subsegment
,technicalsource__c
,created_date__c as registration_start_date
,updated_date__c
,oppname__c as opportunity_name
,eb_subsegment__c as eb_subsegment
,public_institution__c as public_institution
,cs__c
,year_of_foundation__c
,state__c as state_location
,international__c as international
,country__c 
,channel__c as channel
,not_qualified_reason__c as not_qualified_reason
,lead_assignment_assistfield__c
,lead_assignment_assistfield_ii__c
,lead_age__c as lead_age
,of_open_activities__c
,admin_panel_link__c
,title
,opted_out__c as opted_out
,timestamp_changed_to_working__c as working_date
,timestamp_changed_to_qual_disq__c as qualification_date
,days_in_working__c as days_in_working
,first_meeting_date__c
from stg_sfb2b."lead" ;
