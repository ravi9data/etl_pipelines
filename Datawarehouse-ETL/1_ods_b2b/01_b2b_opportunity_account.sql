drop table if exists ods_b2b.opportunity_account;
create table ods_b2b.opportunity_account as 
	select 
			o.opportunity_id,
			o.account_id,
			o.contact_id,
			a.company_id,
			a.customer_id,
			a.created_date as account_created_date,
			o.owner_id as opportunity_owner,
			o.name as opportunity_name,
			o.stage_name,
			o.amount as asv,
			o.csv__c as csv,
			o.weighted_asv__c as weighted_asv,
			o.probability,
			o.created_date,
			o.closed_date,
			o.lastmodifieddate AS last_modified_date,
			o.opportunity_type,
			o.lead_source,
			o.is_closed,
			o.is_won,
			rank() over (partition by o.account_id order by o.created_date ASC) as rank_opportunity,
			count(*) over (partition by o.account_id) as total_account_opportunities,
			o.forecast_cateory as forecast_category,
			o.forecast_category_name,
			o.fiscal_quarter,
			o.fiscal_year,
			o.fiscal,
			o.loss_reason,
			o.usecasedetails__c,
			o.discountamount__c as discount_amount,
			o.is_automated,
			o.is_migrated,
			o.rental_period__c as rental_period,
			o.assist_field_conversion_rate__c as assist_field_conversion_rate,
			u.full_name as owner_name,
			u.segment as owner_segment
			 from ods_b2b.opportunity o
			left join ods_b2b.account a on a.account_id = o.account_id
			left join ods_b2b.user u on u.user_id = o.owner_id;

GRANT SELECT ON ods_b2b.opportunity_account TO tableau;
