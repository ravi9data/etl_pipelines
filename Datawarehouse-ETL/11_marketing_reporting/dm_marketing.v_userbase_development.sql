DROP VIEW IF EXISTS dm_marketing.v_userbase_development;
CREATE VIEW dm_marketing.v_userbase_development AS
WITH countries_sessions AS (
    SELECT
        customer_id,
        CASE WHEN store_name IN ('Austria', 'Spain', 'Germany', 'Netherlands',
                                 'United States') THEN store_name
             WHEN store_name = 'Grover B2B Germany' THEN 'Germany'
             WHEN store_name = 'Grover B2B Spain' THEN 'Spain'
             WHEN store_name = 'Grover B2B Netherlands' THEN 'Netherlands'
             WHEN store_name = 'Grover B2B Austria' THEN 'Austria'
             WHEN geo_country = 'AT' THEN 'Austria'
             WHEN geo_country = 'ES' THEN 'Spain'
             WHEN geo_country = 'DE' THEN 'Germany'
             WHEN geo_country = 'NL' THEN 'Netherlands'
             WHEN geo_country = 'US' THEN 'United States' END AS country,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY session_start) AS row_sessions
    FROM traffic.sessions
    WHERE country IS NOT NULL
      AND customer_id IS NOT NULL
)
   ,potential_churners_base AS (
    SELECT
        subscription_id,
        customer_id,
        minimum_cancellation_date,
        rental_period,
        start_date,
        DATE_TRUNC('month', CURRENT_DATE) AS bom,
        LAST_DAY(CURRENT_DATE) AS eom,
        CASE
            WHEN minimum_cancellation_date::DATE BETWEEN DATE_TRUNC('month', CURRENT_DATE) AND LAST_DAY(CURRENT_DATE)
                THEN TRUE
            ELSE FALSE END                                            AS potential_churners_eoc, --end of contract
        CASE
            WHEN minimum_cancellation_date::DATE < CURRENT_DATE THEN TRUE
            ELSE FALSE END                                            AS potential_churners_ce,  --contract_expired
        ---- weekly
        DATE_TRUNC('week', CURRENT_DATE) AS bow,
        DATEADD('day',6,DATE_TRUNC('week',CURRENT_DATE)::date)::date AS eow,
        CASE
            WHEN minimum_cancellation_date::DATE BETWEEN DATE_TRUNC('week', CURRENT_DATE) AND DATEADD('day',6,DATE_TRUNC('week',CURRENT_DATE)::date)::date
                THEN TRUE
            ELSE FALSE END                                            AS potential_churners_eoc_weekly, --end of contract
        CASE
            WHEN minimum_cancellation_date::DATE < CURRENT_DATE THEN TRUE
            ELSE FALSE END                                            AS potential_churners_ce_weekly  --contract_expired
    FROM master.subscription s
    WHERE status = 'ACTIVE'
)
   ,unique_potential_churners AS (
    SELECT DISTINCT
        bom,
        eom,
        customer_id
    FROM potential_churners_base
    WHERE potential_churners_eoc = TRUE
)
    ,new_users AS (
    SELECT
        DATE_TRUNC('month',c.submitted_date) AS bom,
        LAST_DAY(c.submitted_date::DATE) AS eom,
        customer_type,
        store_country AS country,
        'Active' as customer_label,
        'Active - New Users' as customer_label_detailed,
        'n/a' AS marketing_channel,
        COUNT(DISTINCT order_id) AS customers
    FROM master.order c
    WHERE DATE_TRUNC('month',c.submitted_date) >= DATEADD('month',-24,DATE_TRUNC('month',current_date)) 
        AND paid_orders >= 1 
        AND new_recurring = 'NEW'
    GROUP BY 1,2,3,4,5,6,7  
)
   ,labels_customers AS (
    SELECT DISTINCT
        "date",
        c.customer_id,
        crm_label_braze AS customer_label_detailed,
        CASE 
            WHEN crm_label_braze ilike '%inactive%' THEN 'Inactive'
            WHEN crm_label_braze ilike '%active%' THEN 'Active'
            WHEN crm_label_braze ilike '%registered%' THEN 'Registered'
            WHEN crm_label_braze ilike '%unhealthy%' THEN 'Unhealthy'
        END as customer_label,
        customer_type,
            CASE WHEN c.signup_country <> 'never_add_to_cart' THEN c.signup_country
                 ELSE s.country END AS country
    FROM master.customer_historical c
             LEFT JOIN countries_sessions s
                       ON c.customer_id = s.customer_id
                           AND row_sessions = 1
    WHERE
        (date = LAST_DAY(date) OR date = DATEADD('day',-1,DATE_TRUNC('day',CURRENT_DATE)))
      AND DATE_TRUNC('month',"date") >= DATEADD('month',-24, DATE_TRUNC('month',current_date))     
)
   ,active_and_inactive AS (
    SELECT DISTINCT
        DATE_TRUNC('month',a.date)::DATE AS bom,
        LAST_DAY(a.date)::DATE AS eom,
        a.customer_type,
        a.country,
        a.customer_label,
        a.customer_label_detailed,
        'n/a' AS marketing_channel,
        COUNT(DISTINCT a.customer_id) AS customers
    FROM labels_customers a
             LEFT JOIN master.order c 
             	ON a.customer_id = c.customer_id
             	AND c.paid_orders >= 1 
             	AND c.new_recurring = 'NEW'
             	AND DATE_TRUNC('month',date)::DATE = DATE_TRUNC('month',c.submitted_date)
    WHERE c.customer_id IS NULL -- IN ORDER TO EXCLUDE the NEW customers FROM Active Customers, otherwise, we will count them 2x
    GROUP BY 1,2,3,4,5,6
)
, reactivated_customers_channel AS (
	SELECT 
		p.date,
		p.customer_id,
		o.marketing_channel,
		row_number() over (partition by p.customer_id, p.date order by s.start_date DESC, s.subscription_id) rn
	FROM labels_customers p
	LEFT JOIN master.subscription s 
		ON s.customer_id = p.customer_id
		AND p.date::date >= s.start_date::date
	LEFT JOIN master.ORDER o 
		ON o.order_id = s.order_id
	WHERE p.customer_label_detailed = 'Active - Reactivated' --'Reactivated Customers'
)
,churn_and_reactivated AS (
    SELECT DISTINCT
        DATE_TRUNC('month',a.date)::DATE AS bom,
        LAST_DAY(a.date)::DATE AS eom,
        a.customer_type,
        a.country,
        COALESCE(r.marketing_channel, 'n/a') AS marketing_channel,
        a.customer_label,
        a.customer_label_detailed,
        COUNT(DISTINCT a.customer_id) AS customers
    FROM labels_customers a
    LEFT JOIN reactivated_customers_channel r 
    	ON a.customer_id = r.customer_id 
    	AND a.date = r.date
    	AND r.rn = 1
    LEFT JOIN master.order c 
    	ON a.customer_id = c.customer_id
     	AND c.paid_orders >= 1 
     	AND c.new_recurring = 'NEW'
     	AND DATE_TRUNC('month',a.date)::DATE = DATE_TRUNC('month',c.submitted_date)
    WHERE c.customer_id IS NULL -- IN ORDER TO EXCLUDE the NEW customers FROM Active/Reactivated Customers, otherwise, we will count them 2x
    GROUP BY 1,2,3,4,5,6,7
)
--- weekly
,new_users_weeekly AS ( -- NEW USER IN the 1st week
    SELECT DISTINCT
        DATE_TRUNC('week',c.submitted_date)::date AS bow,
        DATEADD('day',6,DATE_TRUNC('week',c.submitted_date)::date)::date AS eow,
        customer_type,
        store_country AS country,
        'Active' as customer_label,
        'Active - New Users' as customer_label_detailed,
        COUNT(DISTINCT order_id) AS customers
    FROM master.order c
    WHERE DATE_TRUNC('month',c.submitted_date) >= DATEADD('month',-24,DATE_TRUNC('month',current_date)) 
        AND paid_orders >= 1 
        AND new_recurring = 'NEW'
    GROUP BY 1,2,3,4,5,6 
)
 ,labels_customers_weekly AS (
    SELECT DISTINCT
        "date",
        c.customer_id,
        crm_label_braze AS customer_label_detailed,
        CASE 
            WHEN crm_label_braze ilike '%inactive%' THEN 'Inactive'
            WHEN crm_label_braze ilike '%active%' THEN 'Active'
            WHEN crm_label_braze ilike '%registered%' THEN 'Registered'
            WHEN crm_label_braze ilike '%unhealthy%' THEN 'Unhealthy'
        END as customer_label,
        customer_type,
            CASE WHEN c.signup_country <> 'never_add_to_cart' THEN c.signup_country
                 ELSE s.country END AS country
    FROM master.customer_historical c
             LEFT JOIN countries_sessions s
                       ON c.customer_id = s.customer_id
                           AND row_sessions = 1
    WHERE
        (date = DATEADD('day',6,DATE_TRUNC('week',date)::date)::date -- LAST DAY OF week
        	 OR date = DATEADD('day',-1,DATE_TRUNC('day',CURRENT_DATE)))
      AND DATE_TRUNC('month',"date") >= DATEADD('month',-24, DATE_TRUNC('month',current_date))
)
   ,active_and_inactive_weekly AS (
    SELECT DISTINCT
        DATE_TRUNC('week',a.date)::DATE AS bow,
        DATEADD('day',6,DATE_TRUNC('week',a.date)::date)::date AS eow,
        a.customer_type,
        a.country,
        a.customer_label,
        a.customer_label_detailed,
        COUNT(DISTINCT a.customer_id) AS customers
    FROM labels_customers_weekly a
             LEFT JOIN master.order c 
             	ON a.customer_id = c.customer_id
             	AND c.paid_orders >= 1 
             	AND c.new_recurring = 'NEW'
             	AND DATE_TRUNC('week',date)::DATE = DATE_TRUNC('week',c.submitted_date)
    WHERE c.customer_id IS NULL -- IN ORDER TO EXCLUDE the NEW customers FROM Active Customers, otherwise, we will count them 2x
    GROUP BY 1,2,3,4,5,6 
)
   ,churn_and_reactivated_weekly AS (
    SELECT DISTINCT
        DATE_TRUNC('week',a.date)::DATE AS bow,
        DATEADD('day',6,DATE_TRUNC('week',a.date)::date)::date AS eow,
        a.customer_type,
        a.country,
        a.customer_label,
        a.customer_label_detailed,
        COUNT(DISTINCT a.customer_id) AS customers
    FROM labels_customers_weekly a
    LEFT JOIN master.order c 
    	ON a.customer_id = c.customer_id
     	AND c.paid_orders >= 1 
     	AND c.new_recurring = 'NEW'
     	AND DATE_TRUNC('week',a.date)::DATE = DATE_TRUNC('week',c.submitted_date)
    WHERE c.customer_id IS NULL -- IN ORDER TO EXCLUDE the NEW customers FROM Active/Reactivated Customers, otherwise, we will count them 2x
    GROUP BY 1,2,3,4,5,6
)
SELECT
	'monthly' AS report,
    COALESCE(nu.bom, ai.bom, cr.bom)::date date_bom,
    COALESCE(nu.eom, ai.eom, cr.eom)::DATE AS date_eom,
    COALESCE(nu.country,ai.country, cr.country) AS country,
    COALESCE(nu.customer_label,ai.customer_label, cr.customer_label) AS customer_label,
    COALESCE(nu.customer_label_detailed,ai.customer_label_detailed, cr.customer_label_detailed) AS customer_label_detailed,
    COALESCE(nu.customer_type,ai.customer_type, cr.customer_type) AS customer_type,
    COALESCE(nu.marketing_channel, ai.marketing_channel, cr.marketing_channel) AS marketing_channel,
    COALESCE(nu.customers,ai.customers, cr.customers) AS customers
FROM new_users nu
         FULL OUTER JOIN active_and_inactive ai
                         USING (bom, eom, country, customer_label, customer_label_detailed, customer_type, marketing_channel)
         FULL OUTER JOIN churn_and_reactivated cr
                         USING (bom, eom, country, customer_label, customer_label_detailed, customer_type, marketing_channel)                       
UNION 
SELECT
	'weekly' AS report,
    COALESCE(nu.bow, ai.bow, cr.bow)::date date_bom,
    COALESCE(nu.eow, ai.eow, cr.eow)::DATE AS date_eom,
    COALESCE(nu.country,ai.country, cr.country) AS country,
    COALESCE(nu.customer_label,ai.customer_label, cr.customer_label) AS customer_label,
    COALESCE(nu.customer_label_detailed,ai.customer_label_detailed, cr.customer_label_detailed) AS customer_label_detailed,
    COALESCE(nu.customer_type,ai.customer_type, cr.customer_type) AS customer_type,
    'n/a' AS marketing_channel,
    COALESCE(nu.customers,ai.customers, cr.customers) AS customers
FROM new_users_weeekly nu
         FULL OUTER JOIN active_and_inactive_weekly ai
                         USING (bow, eow, country, customer_label, customer_label_detailed, customer_type)
         FULL OUTER JOIN churn_and_reactivated_weekly cr
                         USING (bow, eow, country, customer_label, customer_label_detailed, customer_type)                          
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_marketing.v_userbase_development TO tableau;
