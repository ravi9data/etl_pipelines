DROP TABLE IF EXISTS dm_b2b.gbp_intercom;

CREATE TABLE dm_b2b.gbp_intercom AS WITH conversations AS (
    SELECT
        icp.conversation_id,
        icp.conversation_part_assigned_to_id,
        CASE
            WHEN icp.conversation_part_assigned_to_id = 2351837
                THEN 'B2B'
            WHEN icp.conversation_part_assigned_to_id = 5610605
                THEN 'GBP'
            ELSE NULL
        END AS assigned_to_team
    FROM ods_data_sensitive.intercom_conversation_parts icp
    WHERE
        icp.conversation_part_assigned_to_id IN ('2351837', '5610605')
        AND icp.conversation_created_at :: DATE >= '2022-09-27'
)
SELECT
    icp.conversation_id,
    icp.conversation_part_assigned_to_id,
    icp.assigned_to_team,
    ifc.contact_reason,
    ifc.created_at,
    ifc.intercom_customer_id,
    ifc.market,
    ifc.state,
    ifc.conversation_rating,
    ifc.time_to_admin_reply
FROM conversations icp
INNER JOIN ods_data_sensitive.intercom_first_conversation ifc
    ON icp.conversation_id = ifc.id;

GRANT
SELECT
    ON dm_b2b.gbp_intercom TO tableau;