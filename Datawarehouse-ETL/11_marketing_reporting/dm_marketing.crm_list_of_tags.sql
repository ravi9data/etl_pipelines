DROP TABLE IF EXISTS dm_marketing.crm_list_of_tags;
CREATE TABLE dm_marketing.crm_list_of_tags AS
WITH RECURSIVE tmp_tags (all_tag, num_actions, idx, tags) AS (
    SELECT
        all_tag,
        regexp_count(all_tag, ',') + 1 AS num_actions,
        1 AS idx,
        split_part(all_tag, ',', 1) AS tags
    FROM (
             SELECT DISTINCT
                 LOWER(REPLACE(TRANSLATE(LOWER(tags), '[]', ''), '"', '')::VARCHAR(100)) AS all_tag
             FROM stg_external_apis.braze_canvas_details
             UNION ALL
             SELECT DISTINCT
                 LOWER(REPLACE(TRANSLATE(LOWER(tags), '[]', ''), '"', '')::VARCHAR(100)) AS all_tag
             FROM stg_external_apis.braze_campaign_details
         )

    UNION ALL

    SELECT
        a.all_tag,
        a.num_actions,
        a.idx + 1 AS idx,
        TRIM(SPLIT_PART(a.all_tag, ',', a.idx + 1)) AS tags
    FROM
        tmp_tags a
    WHERE a.idx < a.num_actions
)

SELECT DISTINCT all_tag, tags
FROM tmp_tags
WHERE COALESCE(tags,'') <> '';
