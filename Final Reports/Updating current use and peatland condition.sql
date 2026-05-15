BEGIN;

-- INSERT CURRENT USE FROM FINAL REPORTS
WITH current_use_all AS (
    SELECT
        trim(grant_id) AS grant_id,
        CASE
            WHEN current_use ILIKE '%Other%'
                THEN regexp_replace(current_use, '[^\w\s^,]', '', 'g') || ' - ' || notes
            ELSE regexp_replace(current_use, '[^\w\s^,]', '', 'g')
        END AS current_use
    FROM pa_final_report.site_outline

    UNION

    SELECT
        trim(grant_reference) AS grant_id,
        current_use_of_site AS current_use
    FROM public.site_summary_2021
),

grant_id_unnest AS (
    SELECT
        centroid,
        trim(unnest(string_to_array(grant_id, ','))) AS grant_id
    FROM pa_ghg_reporting.ghg_report_2026_20260514
),

join_use_grant_id AS (
    SELECT
        a.centroid,
        a.grant_id,
        b.current_use
    FROM grant_id_unnest a
    LEFT JOIN current_use_all b
        ON a.grant_id = b.grant_id
),

grant_id_nest AS (
    SELECT
        centroid,
        string_agg(DISTINCT grant_id, ',' ORDER BY grant_id) AS grant_id,
        string_agg(DISTINCT current_use, ',' ORDER BY current_use) FILTER (WHERE current_use IS NOT NULL) AS current_use
    FROM join_use_grant_id
    GROUP BY centroid
)

UPDATE pa_ghg_reporting.ghg_report_2026_20260514 a
SET pa_current_use = b.current_use
FROM grant_id_nest b
WHERE a.centroid = b.centroid;


-- CLEAN OR UPDATE OLD CURRENT USE CATEGORIES
UPDATE pa_ghg_reporting.ghg_report_2026_20260514
SET pa_current_use = 'Forestry'
WHERE pa_current_use = '5,4';

UPDATE pa_ghg_reporting.ghg_report_2026_20260514
SET pa_current_use = replace(pa_current_use, '5,4', 'Forestry')
WHERE pa_current_use LIKE '%5,4%';

UPDATE pa_ghg_reporting.ghg_report_2026_20260514
SET pa_current_use = replace(pa_current_use, '1,3,4', '')
WHERE pa_current_use LIKE '%1,3,4%';

UPDATE pa_ghg_reporting.ghg_report_2026_20260514
SET pa_current_use = replace(pa_current_use, '3,4,1', '')
WHERE pa_current_use LIKE '%3,4,1%';

UPDATE pa_ghg_reporting.ghg_report_2026_20260514
SET pa_current_use = replace(pa_current_use, '1,4', '')
WHERE pa_current_use LIKE '%1,4%';

UPDATE pa_ghg_reporting.ghg_report_2026_20260514
SET pa_current_use = replace(pa_current_use, '1,', '')
WHERE pa_current_use LIKE '%1,%';

UPDATE pa_ghg_reporting.ghg_report_2026_20260514
SET pa_current_use = replace(pa_current_use, '4', 'Deer Management')
WHERE pa_current_use = '4';


-- INSERT CONDITIONS FROM FINAL REPORTS
WITH conditions_all AS (
    SELECT
        trim(grant_id) AS grant_id,
        CASE
            WHEN peatland_condition ILIKE '%Other%'
                THEN regexp_replace(peatland_condition, '[^\w\s^,]', '', 'g') || ' - ' || notes
            ELSE regexp_replace(peatland_condition, '[^\w\s^,]', '', 'g')
        END AS peatland_condition
    FROM pa_final_report.site_outline

    UNION

    SELECT
        trim(grant_reference) AS grant_id,
        peat_condition_data AS peatland_condition
    FROM public.site_summary_2021
),

grant_id_unnest AS (
    SELECT
        centroid,
        trim(unnest(string_to_array(grant_id, ','))) AS grant_id
    FROM pa_ghg_reporting.ghg_report_2026_20260514
),

join_condition_grant_id AS (
    SELECT
        a.centroid,
        a.grant_id,
        b.peatland_condition
    FROM grant_id_unnest a
    LEFT JOIN conditions_all b
        ON a.grant_id = b.grant_id
),

grant_id_nest AS (
    SELECT
        centroid,
        string_agg(DISTINCT grant_id, ',' ORDER BY grant_id) AS grant_id,
        string_agg(DISTINCT peatland_condition, ',' ORDER BY peatland_condition)
            FILTER (WHERE peatland_condition IS NOT NULL) AS peatland_condition
    FROM join_condition_grant_id
    GROUP BY centroid
)

UPDATE pa_ghg_reporting.ghg_report_2026_20260514 a
SET pa_condition_category = b.peatland_condition
FROM grant_id_nest b
WHERE a.centroid = b.centroid;

COMMIT;