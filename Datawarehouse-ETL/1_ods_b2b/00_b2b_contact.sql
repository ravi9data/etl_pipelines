drop table if exists ods_b2b.contact;
create table ods_b2b.contact as 
select 
id as contact_id
,accountid as account_id
,lastname as last_name
,firstname as first_name
,salutation as salutation
,name as full_name
,mailingstreet
,mailingcity
,mailingstate
,mailingpostalcode
,mailingcountry
,phone
,mobilephone
,email
,title
,department
,leadsource as lead_source
,ownerid as owner_id
,createddate as created_date
,createdbyid as created_id
,lastmodifieddate
,lastmodifiedbyid
,systemmodstamp
,lastactivitydate
,lastvieweddate
,lastreferenceddate
,photourl
,role__c as contact_role
,formalinformal__c
,communicationchannel__c
,language__c
,customerid__c as customer_id
,islegalrep__c as is_legal_rep
,legacy_contact_owner__c as legacy_owner
,migrated__c as is_migrated
,email_2__c as secondary_email
,birthdate__c 
		from stg_sfb2b.contact;