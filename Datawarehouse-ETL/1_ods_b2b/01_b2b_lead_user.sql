drop table if exists ods_b2b.lead_user;
create table ods_b2b.lead_user as 
select  lead_id
			,company
			,l.customer_id
			,l.website
			,leadsource
			,lead_status
			,channel
			,company_status__c as company_status
			,l.company_id
			,company_lead_type
			,l.industry
			,lead_owner
			,is_converted
			,converted_date
			,converted_account_id
			,a.legal_entity_type
			,a.account_differentiation
			,converted_contact_id
			,converted_opportunity_id
			,l.created_date
			,l.lastmodifieddate
			,employees
			legal_type
			,l.is_migrated
			,is_legal_rep
			,new_lead_segment
			,l.su_subsegment
			,opportunity_name
			,l.eb_subsegment
			,l.public_institution
			,l.year_of_foundation__c as year_of_foundation
			,l.state_location
			,l.international
			,l.country__c
			,l.not_qualified_reason
			,l.lead_age
			,l.opted_out
			,l.working_date
			,l.qualification_date
			,l.days_in_working
			,l.first_meeting_date__c
			,u.full_name as owner_name
			,u.segment as owner_segment
		from ods_b2b.lead l 
		left join ods_b2b.user u on u.user_id = l.lead_owner
		left join ods_b2b.account a on a.account_id = l.converted_account_id
		order by lastmodifieddate DESC ;
		
GRANT SELECT ON ods_b2b.lead_user TO redash_pricing;
GRANT SELECT ON ods_b2b.lead_user TO tableau;
