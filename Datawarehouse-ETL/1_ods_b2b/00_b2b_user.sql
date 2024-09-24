drop table if exists ods_b2b.user;
create table ods_b2b.user as
select 
	id as user_id
,username as user_name
,lastname as last_name
,firstname as first_name
,name as full_name
,companyname as company_name
,division
,department
,title
,email
,mobilephone
,alias
,communitynickname
,isactive
,timezonesidkey
,userroleid as userrole_id
,profileid as profile_id
,usertype as user_type
,createddate as created_date
,createdbyid as created_id
,lastmodifieddate
,salestarget__c as sales_target
,asv_target__c as asv_target
,segment__c	as segment
	from stg_sfb2b."user";

GRANT SELECT ON ods_b2b.user TO tableau;
GRANT SELECT ON ods_b2b.user TO b2b_redash;
