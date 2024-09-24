DROP TABLE IF EXISTS tmp_bi_ods_store;
CREATE TEMP TABLE tmp_bi_ods_store AS 

 WITH
 offline_employees AS (
 SELECT store_id,
	COUNT(CASE WHEN group_id = 2 THEN auth_user_id END) AS store_managers,
	COUNT(CASE WHEN group_id = 3 THEN auth_user_id END) AS store_employees,
	COUNT(CASE WHEN group_id IS NULL THEN auth_user_id END) AS employees_unmapped
	FROM  stg_api_production.employees e
	LEFT JOIN stg_flowing_mosel.auth_user_groups u
---user id and group id
	ON e.auth_user_id=u.user_id
	GROUP BY 1),
 a AS (
         SELECT DISTINCT store.id,
         		code.storenumber AS store_number,
				store.code,
         		code.zipcode,
                CASE
                    WHEN replace(lower(store.name::TEXT), ' '::TEXT, ''::TEXT) LIKE '%mediamarkt%'::TEXT
                       OR p.id=3 OR store.id IN (635, 632) THEN 'Media Markt'::TEXT
                    WHEN replace(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) LIKE '%gravis%'::TEXT
    				  OR p.id=26 THEN 'Gravis'::TEXT
                    WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) LIKE '%saturn%'::TEXT
                      or p.id =36 THEN 'Saturn'::TEXT
                    WHEN (REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) LIKE '%tchibo%'::TEXT
                           AND store.name::TEXT <> 'Tchibo Affiliate'::TEXT)
     				 	THEN 'Tchibo'::TEXT
                    WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) LIKE '%conrad%'::TEXT
                     OR p.id=23 THEN 'Conrad'::TEXT
                       WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) = 'germany'::TEXT
                     THEN 'Grover - Germany'::TEXT
                    WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) LIKE '%groverb2b%'::TEXT
                     THEN 'Grover - '::TEXT + c.name
                    WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) = 'uk' THEN 'Grover - UK'::TEXT
                    WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) = 'austria' THEN 'Grover - Austria'::TEXT
                    WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT)  = 'netherlands' THEN 'Grover - Netherlands'::TEXT
                    WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT)  = 'spain' THEN 'Grover - Spain'::TEXT
                    WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) = 'usa' THEN 'Grover - USA old'::TEXT
                    WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) = 'unitedstates' OR store.id = 621 THEN 'Grover - United States'::TEXT
                    WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) LIKE '%thalia%'::text
                     or p.id=31 THEN 'Thalia'::TEXT
                   WHEN LOWER(store.name::text) = 'quelle austria' OR
	                    (REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) LIKE '%otto austria%'::TEXT  OR p.id=34) OR
                        LOWER(store.name::TEXT) = 'universal austria'
	                    THEN 'UNITO'::TEXT
                   WHEN LOWER(store.name::TEXT) = 'aldi talk' THEN 'Aldi Talk'::TEXT
                   WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) = 'weltbild' THEN 'Weltbild'::TEXT
                   WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) = 'comspot'
                   OR p.id=50 THEN 'Comspot'::TEXT
                    WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) = 'samsung' THEN 'Samsung'::TEXT
                    WHEN replace(lower(store.name::TEXT), ' '::TEXT, ''::TEXT) LIKE 'expert%'::TEXT
                     OR p.id=46 THEN 'Expert'::TEXT
                     WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) LIKE '%irobot%'::TEXT
                      THEN 'iRobot'::TEXT
                      WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) LIKE '%shifter%'::TEXT
                      THEN 'Shifter'::TEXT
                 	WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) LIKE '%euronics%'
                 	OR p.id=35 THEN 'Euronics'::TEXT
                 	WHEN replace(lower(store.name::TEXT), ' '::TEXT, ''::TEXT) LIKE '%betterworx%'
                 	THEN 'Betterworx'::TEXT
                  when REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) ILIKE '%Mobilcom%'
                 	then 'Mobilcom Debitel'::TEXT
                 	when REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) ILIKE '%Baur%'
                 	then 'Baur'::TEXT
                  WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) ILIKE '%BoltShopNL%'::TEXT
                     THEN 'BoltShop NL'::TEXT
                   WHEN REPLACE(LOWER(store.name::TEXT), ' '::TEXT, ''::TEXT) ILIKE '%bobAustria%'::TEXT
                     THEN 'Bob AT'::TEXT
                    ELSE 'Others'::TEXT
                END AS account_name,
          CASE WHEN store.id = 20 THEN 'Media Markt'
		        ELSE store.name END AS store_name,
  			REPLACE(REPLACE(REPLACE(store_name,'ü','ue'),'ö','oe'),'ä','ae') AS store_name_normalized,
                CASE
                    WHEN store.offline THEN 'offline'::TEXT
                    ELSE 'online'::TEXT
                END AS store_type,
           c.name AS country_name,
          store.created_at AS created_date,
     GREATEST(store.created_at, store.updated_at) AS updated_date
           FROM  stg_api_production.spree_stores store
   		   LEFT JOIN stg_api_production.partners p
				ON store.partner_id=p.id
   		   LEFT JOIN stg_api_production.spree_countries c
   		   		on c.id=store.country_id
   	   	LEFT JOIN public.partner_store_codes code
   	   		ON code.backofficestoreid = store.id
        )
 SELECT
    a.id,
 	a.store_number,
 	a.zipcode,
	a.code AS store_code,
 --	s.ort as city,
 --	s.bundesland as state,
    a.account_name,
     (CASE  WHEN a.account_name='Media Markt' AND REPLACE(LOWER(a.store_name::TEXT), ' '::TEXT, ''::TEXT) NOT LIKE '%mediamarkt%'::TEXT
     		THEN 'Mediamarkt '||a.store_name
     		WHEN a.account_name='Conrad' AND REPLACE(LOWER(a.store_name::TEXT), ' '::TEXT, ''::TEXT) NOT LIKE '%conrad%'::TEXT
     		THEN 'Conrad '||a.store_name
      		WHEN a.store_name in ('Saturn Kiel','Saturn Norderstedt') THEN a.store_name||a.store_number
      		WHEN a.account_name='Expert' AND REPLACE(LOWER(a.store_name::TEXT), ' '::TEXT, ''::TEXT) NOT LIKE '%expert%'::TEXT
     		THEN 'Expert '||a.store_name
     			ELSE a.store_name END)  AS store_name,
     LOWER(CASE  WHEN a.account_name='Media Markt' AND REPLACE(LOWER(a.store_name_normalized::TEXT), ' '::TEXT, ''::TEXT) NOT LIKE '%mediamarkt%'::TEXT
     		THEN 'Mediamarkt '||a.store_name_normalized
     		WHEN a.account_name='Conrad' AND REPLACE(LOWER(a.store_name_normalized::TEXT), ' '::TEXT, ''::TEXT) NOT LIKE '%conrad%'::TEXT
     		THEN 'Conrad '||a.store_name_normalized
           WHEN a.store_name IN ('Saturn Kiel','Saturn Norderstedt') THEN a.store_name||a.store_number
     			ELSE a.store_name_normalized END)  AS store_name_normalized,
    a.store_type,
    (a.account_name || ' '::TEXT) || a.store_type AS store_label,
        CASE
            WHEN a.country_name NOT LIKE 'Germany' AND
              LOWER(a.account_name) LIKE '%grover%'::TEXT THEN 'Grover International'::TEXT
            WHEN LOWER(a.account_name) LIKE '%grover%'::TEXT THEN 'Grover'::TEXT
            WHEN a.store_type = 'offline'::TEXT THEN 'Partners Offline'::TEXT
            WHEN a.store_type = 'online'::TEXT THEN 'Partners Online'::TEXT
            ELSE NULL::TEXT
        END AS store_short,
        country_name,
        created_date,
     updated_date,
     oe.store_managers,
     oe.store_employees,
     oe.employees_unmapped
   FROM a
   LEFT JOIN offline_employees oe ON a.id = oe.store_id
;

BEGIN TRANSACTION;

DELETE FROM bi_ods.store WHERE 1=1;

INSERT INTO bi_ods.store
SELECT * FROM tmp_bi_ods_store;

END TRANSACTION;