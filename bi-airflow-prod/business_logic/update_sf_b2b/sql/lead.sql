SELECT
	lead_id AS Id,
	live_asv AS Live_ASV__c,
	live_csv AS Live_CSV__c,
	sub_value AS All_time_ASV__c,
	committed_sub_value as All_time_CSV__c
FROM
	dm_b2b.lead_sync ls
