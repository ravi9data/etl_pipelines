drop table if exists ods_production.system_changes;
create table ods_production.system_changes as
with main_data as 
			(select 
				'Subscription' as entity, 
				parentid as entity_id,
				createdbyid,
				createddate as created_date,
				field as field_type,
				oldvalue as old_value,
				newvalue as newvalue
				from stg_salesforce.subscription_history
				UNION ALL
			select 
				 'Asset' as entity, 
				assetid as entity_id,
				createdbyid,
				createddate as created_date,
				field as field_type,
				oldvalue as old_value,
				newvalue as newvalue
				from stg_salesforce.asset_history
				UNION ALL	
			select 
				 'Allocation' as entity, 
				parentid as entity_id,
				createdbyid,
				createddate as created_date,
				field as field_type,
				oldvalue as old_value,
				newvalue as newvalue
				from stg_salesforce.customer_asset_allocation__history
				UNION ALL
			select 
				 'Asset Payment' as entity, 
				parentid as entity_id,
				createdbyid,
				createddate as created_date,
				field as field_type,
				oldvalue as old_value,
				newvalue as newvalue
				from stg_salesforce.asset_payment_history
				UNION ALL
			select 
				 'Subscription Payment' as entity, 
				parentid as entity_id,
				createdbyid,
				createddate as created_date,
				field as field_type,
				oldvalue as old_value,
				newvalue as newvalue
				from stg_salesforce.asset_payment_history
				UNION ALL
			select 
				 'Order' as entity, 
				orderid as entity_id,
				createdbyid,
				createddate as created_date,
				field as field_type,
				oldvalue as old_value,
				newvalue as newvalue
				from stg_salesforce.order_history)
				/*, I am moving this calculation to dm layer.	
		  user_data as 		
				(select id,name, 
				case when name in('DA & IT Backend Grover', 'DA & IT Grover', 'Tech Script', 'Tech Backend','IT Grover','NodeOps Backend')
				then 'System'
				else 'Manual' end as user_type
	 			from 
				stg_salesforce."user") */
		select *
		   from main_data m ;
		   --left join user_data u on m.createdbyid = u.id;

GRANT SELECT ON ods_production.system_changes TO operations_redash;
GRANT SELECT ON ods_production.system_changes TO tableau;
GRANT SELECT ON dm_operations.system_changes TO tableau;
GRANT SELECT ON dm_operations.system_changes TO GROUP recommerce;
