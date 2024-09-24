 drop table if exists ods_b2b.task;
 create table ods_b2b.task as 
 select id as task_id
	 ,whoid
	 ,whatid
	 ,subject
	 ,activitydate
	 ,status
	 ,priority
	 ,ishighpriority
	 ,ownerid
	 ,description
	 ,accountid
	 ,createddate
	 ,createdbyid
	 ,lastmodifieddate
	 ,lastmodifiedbyid
	 ,reminderdatetime 
	 ,completeddatetime as completed_time
	 ,opportunitystage__c as opportunity_stage
	 ,assistfield_assigned_to_role__c as assist_field
	 from stg_sfb2b.task order by activitydate DESC ;