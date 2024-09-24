drop table if exists ods_b2b.event;
create table ods_b2b.event as 
		 select id as event_id,
			 whoid as lead_id,
			 whatid as what_id,
			 subject,
			 location,
			 activitydate as activity_date,
			 activitydatetime as activity_time,
			 startdatetime as start_time,
			 enddatetime as end_time,
			 durationinminutes as duration,
			 description,
			 accountid  as account_id,
			 ownerid as ownerider_id,
			 createddate as created_date,
			 createdbyid as created_id,
			 lastmodifieddate as last_modified_date,
			 assistfield_assigned_to_role__c as assist_field
			 from 
			 stg_sfb2b.event;