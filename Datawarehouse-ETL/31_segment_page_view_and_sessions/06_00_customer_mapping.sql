
DROP TABLE IF EXISTS segment.customer_mapping_web;
CREATE TABLE segment.customer_mapping_web AS
WITH exclude_anonymous_id_without_customer_id AS (
    SELECT 
        anonymous_id, 
        COUNT(user_id) AS total_users
    FROM segment.all_events
    WHERE platform = 'web'
    GROUP BY 1
    HAVING total_users >= 1
),

user_mapping AS (
    SELECT
        a.event_id,
        a.anonymous_id,
        COALESCE(FIRST_VALUE(a.user_id) IGNORE NULLS OVER
            (PARTITION BY a.anonymous_id, a.event_time::DATE ORDER BY a.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                 FIRST_VALUE(a.user_id) IGNORE NULLS OVER
                     (PARTITION BY a.anonymous_id ORDER BY a.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS customer_id
    FROM segment.all_events a
        LEFT JOIN exclude_anonymous_id_without_customer_id b USING (anonymous_id) 
    WHERE a.platform = 'web'
        AND b.anonymous_id IS NOT NULL
)

SELECT DISTINCT a.event_id,
       a.anonymous_id,
       a.customer_id,
       c.created_at AS user_registration_date,
       c.start_date_of_first_subscription AS customer_acquisition_date
FROM user_mapping a
    LEFT JOIN master.customer c ON c.customer_id = a.customer_id;

DROP TABLE IF EXISTS segment.customer_mapping_app;
CREATE TABLE segment.customer_mapping_app AS
WITH anonymous_id_with_customer_id AS (
    SELECT
        anonymous_id,
        COUNT(user_id) AS total_users
    FROM react_native.screens
    GROUP BY 1
    HAVING total_users >= 1
),

     user_mapping AS (
         SELECT
             a.id,
             a.anonymous_id,
             COALESCE(FIRST_VALUE(a.user_id) IGNORE NULLS OVER
                 (PARTITION BY a.anonymous_id, a.timestamp::DATE ORDER BY a.timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                      FIRST_VALUE(a.user_id) IGNORE NULLS OVER
                          (PARTITION BY a.anonymous_id ORDER BY a.timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS customer_id
         FROM react_native.screens a
                  LEFT JOIN anonymous_id_with_customer_id b USING (anonymous_id)
         WHERE b.anonymous_id IS NOT NULL
     )

SELECT DISTINCT a.id,
                a.anonymous_id,
                a.customer_id::INT AS customer_id,
                c.created_at AS user_registration_date,
                c.start_date_of_first_subscription AS customer_acquisition_date
FROM user_mapping a
         LEFT JOIN master.customer c ON c.customer_id = a.customer_id;


GRANT SELECT ON segment.customer_mapping_web TO tableau;
GRANT SELECT ON segment.customer_mapping_app TO tableau;
GRANT SELECT ON segment.customer_mapping_web TO hams;
GRANT SELECT ON segment.customer_mapping_web TO hams;