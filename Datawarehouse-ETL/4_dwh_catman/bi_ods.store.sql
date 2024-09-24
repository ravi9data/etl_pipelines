DROP TABLE IF EXISTS tmp_bi_ods_store;
CREATE TEMP TABLE tmp_bi_ods_store AS 

 WITH
 offline_employees as (
 select store_id,
	count(case when group_id = 2 then auth_user_id end) as store_managers,
	count(case when group_id = 3 then auth_user_id end) as store_employees,
	count(case when group_id is null then auth_user_id end) as employees_unmapped
	from  stg_api_production.employees e
	left join stg_flowing_mosel.auth_user_groups u
---user id and group id
	on e.auth_user_id=u.user_id
	group by 1),
 a AS (
         SELECT DISTINCT store.id,
         		code.storenumber as store_number,
				store.code,
         		code.zipcode,
                CASE
                    WHEN replace(lower(store.name::text), ' '::text, ''::text) like '%mediamarkt%'::text
                       or p.id=3 or store.id in (635, 632) THEN 'Media Markt'::text
                    WHEN replace(lower(store.name::text), ' '::text, ''::text) like '%gravis%'::text
    				  or p.id=26 THEN 'Gravis'::text
                    WHEN replace(lower(store.name::text), ' '::text, ''::text) like '%saturn%'::text
                      or p.id =36 THEN 'Saturn'::text
                    WHEN (replace(lower(store.name::text), ' '::text, ''::text) like '%tchibo%'::text
                           AND store.name::text <> 'Tchibo Affiliate'::text)
     				 	THEN 'Tchibo'::text
                    WHEN replace(lower(store.name::text), ' '::text, ''::text) like '%conrad%'::text
                     or p.id=23 THEN 'Conrad'::text
                       WHEN replace(lower(store.name::text), ' '::text, ''::text) = 'germany'::text
                     THEN 'Grover - Germany'::text
                     THEN 'Grover - '::text + c.name
                    WHEN replace(lower(store.name::text), ' '::text, ''::text) = 'uk' THEN 'Grover - UK'::text
                    WHEN replace(lower(store.name::text), ' '::text, ''::text) = 'austria' THEN 'Grover - Austria'::text
                    WHEN replace(lower(store.name::text), ' '::text, ''::text)  = 'netherlands' THEN 'Grover - Netherlands'::text
                    WHEN replace(lower(store.name::text), ' '::text, ''::text)  = 'spain' THEN 'Grover - Spain'::text
                    WHEN replace(lower(store.name::text), ' '::text, ''::text) = 'usa' THEN 'Grover - USA old'::text
                    WHEN replace(lower(store.name::text), ' '::text, ''::text) = 'unitedstates' or store.id = 621 THEN 'Grover - United States'::text
                    WHEN replace(lower(store.name::text), ' '::text, ''::text) like '%thalia%'::text
                     or p.id=31 THEN 'Thalia'::text
                   WHEN lower(store.name::text) = 'quelle austria' OR
	                    (replace(lower(store.name::text), ' '::text, ''::text) like '%otto austria%'::text  or p.id=34) OR
                        lower(store.name::text) = 'universal austria'
	                    THEN 'UNITO'::text
                   WHEN lower(store.name::text) = 'aldi talk' THEN 'Aldi Talk'::text
                   WHEN replace(lower(store.name::text), ' '::text, ''::text) = 'weltbild' THEN 'Weltbild'::text
                   WHEN replace(lower(store.name::text), ' '::text, ''::text) = 'comspot'
                   or p.id=50 THEN 'Comspot'::text
                    WHEN replace(lower(store.name::text), ' '::text, ''::text) = 'samsung' THEN 'Samsung'::text
                    WHEN replace(lower(store.name::text), ' '::text, ''::text) like 'expert%'::text
                     or p.id=46 THEN 'Expert'::text
                     WHEN replace(lower(store.name::text), ' '::text, ''::text) like '%irobot%'::text
                      THEN 'iRobot'::text
                      WHEN replace(lower(store.name::text), ' '::text, ''::text) like '%shifter%'::text
                      THEN 'Shifter'::text
                 	WHEN replace(lower(store.name::text), ' '::text, ''::text) like '%euronics%'
                 	or p.id=35 THEN 'Euronics'::text
                 	WHEN replace(lower(store.name::text), ' '::text, ''::text) like '%betterworx%'
                 	THEN 'Betterworx'::text
                  when replace(lower(store.name::text), ' '::text, ''::text) ilike '%Mobilcom%'
                 	then 'Mobilcom Debitel'::text
                 	when replace(lower(store.name::text), ' '::text, ''::text) ilike '%Baur%'
                 	then 'Baur'::text
                  WHEN replace(lower(store.name::text), ' '::text, ''::text) ilike '%BoltShopNL%'::text
                     THEN 'BoltShop NL'::text
                   WHEN replace(lower(store.name::text), ' '::text, ''::text) ilike '%bobAustria%'::text
                     THEN 'Bob AT'::text
                    ELSE 'Others'::text
                END AS account_name,
          case when store.id = 20 then 'Media Markt'
		        else store.name end AS store_name,
  			replace(replace(replace(store_name,'ü','ue'),'ö','oe'),'ä','ae') AS store_name_normalized,
                CASE
                    WHEN store.offline THEN 'offline'::text
                    ELSE 'online'::text
                END AS store_type,
           c.name as country_name,
          store.created_at as created_date,
     greatest(store.created_at, store.updated_at) as updated_date
           FROM  stg_api_production.spree_stores store
   		   left join stg_api_production.partners p
				on store.partner_id=p.id
   		   left join stg_api_production.spree_countries c
   		   		on c.id=store.country_id
   	   	left join public.partner_store_codes code
   	   		on code.backofficestoreid = store.id
        )
 SELECT
    a.id,
 	a.store_number,
 	a.zipcode,
	a.code as store_code,
 --	s.ort as city,
 --	s.bundesland as state,
    a.account_name,
     (case  when a.account_name='Media Markt' and replace(lower(a.store_name::text), ' '::text, ''::text) not like '%mediamarkt%'::text
     		then 'Mediamarkt '||a.store_name
     		when a.account_name='Conrad' and replace(lower(a.store_name::text), ' '::text, ''::text) not like '%conrad%'::text
     		then 'Conrad '||a.store_name
      		when a.store_name in ('Saturn Kiel','Saturn Norderstedt') then a.store_name||a.store_number
      		when a.account_name='Expert' and replace(lower(a.store_name::text), ' '::text, ''::text) not like '%expert%'::text
     		then 'Expert '||a.store_name
     			else a.store_name end)  AS store_name,
     lower(case  when a.account_name='Media Markt' and replace(lower(a.store_name_normalized::text), ' '::text, ''::text) not like '%mediamarkt%'::text
     		then 'Mediamarkt '||a.store_name_normalized
     		when a.account_name='Conrad' and replace(lower(a.store_name_normalized::text), ' '::text, ''::text) not like '%conrad%'::text
     		then 'Conrad '||a.store_name_normalized
           when a.store_name in ('Saturn Kiel','Saturn Norderstedt') then a.store_name||a.store_number
     			else a.store_name_normalized end)  AS store_name_normalized,
    a.store_type,
    (a.account_name || ' '::text) || a.store_type AS store_label,
        CASE
            WHEN a.country_name not like 'Germany' and
            WHEN a.store_type = 'offline'::text THEN 'Partners Offline'::text
            WHEN a.store_type = 'online'::text THEN 'Partners Online'::text
            ELSE NULL::text
        END AS store_short,
        country_name,
        created_date,
     updated_date,
     oe.store_managers,
     oe.store_employees,
     oe.employees_unmapped
   FROM a
   left join offline_employees oe on a.id = oe.store_id
;

BEGIN TRANSACTION;

DELETE FROM bi_ods.store WHERE 1=1;

INSERT INTO bi_ods.store
SELECT * FROM tmp_bi_ods_store;

END TRANSACTION;
