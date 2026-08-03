---REMEMBER TO REPLACE THE NAME OF THE GHG TABLE AT THE BOTTOM OF THIS BIG QUERY
--- THE GGHG TABLE WAS CREATED WITH STEPS A TO H


DROP TABLE IF EXISTS pa_ghg_reporting.first_ghg_report_2026_with_unespecied;
CREATE TABLE pa_ghg_reporting.first_ghg_report_2026_with_unespecied AS
SELECT 
    *,
    
    -- Actively eroding outside peat map
    (-1) * "difference" * (
        (
            COALESCE("% Actively eroding: Flat bare", 0) +
            COALESCE("% Actively eroding", 0) +
            COALESCE("% Actively eroding: Hagg/gully", 0) +
            COALESCE("% Drained: Hagg/gully", 0)
        ) / 100.0
    ) AS "Actively eroding outside peatmap",
    
    -- Modified outside peat map
    (-1) * "difference" * (
        (
            COALESCE("% Modified", 0) +
            COALESCE("% Drained: Artificial", 0) +
            COALESCE("% Drained", 0) +
            COALESCE("% Near natural", 0) +
            COALESCE("% Not provided", 0) +
            COALESCE("% No peat", 0) +
            COALESCE("% NA", 0)
        ) / 100.0
    ) AS "Modified outside peat map",
    
    -- Forest outside peat map
    (-1) * "difference" * (
        COALESCE("% Forested / previously forested", 0) / 100.0
    ) AS "Forest outside peat map",
    
    -- Unspecified (calculated after previous 3)
    (-1) * "difference" 
    - (
        (-1) * "difference" * (
            (
                COALESCE("% Actively eroding: Flat bare", 0) +
                COALESCE("% Actively eroding", 0) +
                COALESCE("% Actively eroding: Hagg/gully", 0) +
                COALESCE("% Drained: Hagg/gully", 0)
            ) / 100.0
        ) +
        (-1) * "difference" * (
            (
                COALESCE("% Modified", 0) +
                COALESCE("% Drained: Artificial", 0) +
                COALESCE("% Drained", 0) +
                COALESCE("% Near natural", 0) +
                COALESCE("% Not provided", 0) +
                COALESCE("% No peat", 0) +
                COALESCE("% NA", 0)
            ) / 100.0
        ) +
        (-1) * "difference" * (
            COALESCE("% Forested / previously forested", 0) / 100.0
        )
    ) AS "Unspecified",
    
    -- Unspecified assigned to Forest
    CASE 
        WHEN forestry = TRUE THEN 
            (-1) * "difference" 
            - (
                (-1) * "difference" * (
                    (
                        COALESCE("% Actively eroding: Flat bare", 0) +
                        COALESCE("% Actively eroding", 0) +
                        COALESCE("% Actively eroding: Hagg/gully", 0) +
                        COALESCE("% Drained: Hagg/gully", 0)
                    ) / 100.0
                ) +
                (-1) * "difference" * (
                    (
                        COALESCE("% Modified", 0) +
                        COALESCE("% Drained: Artificial", 0) +
                        COALESCE("% Drained", 0) +
                        COALESCE("% Near natural", 0) +
                        COALESCE("% Not provided", 0) +
                        COALESCE("% No peat", 0) +
                        COALESCE("% NA", 0)
                    ) / 100.0
                ) +
                (-1) * "difference" * (
                    COALESCE("% Forested / previously forested", 0) / 100.0
                )
            )
        ELSE 0
    END AS "Unspecified assigned to Forest",
    
    -- Unspecified assigned to Modified
    CASE 
        WHEN forestry = FALSE THEN 
            (
                (
                    (-1) * "difference" 
                    - (
                        (-1) * "difference" * (
                            (
                                COALESCE("% Actively eroding: Flat bare", 0) +
                                COALESCE("% Actively eroding", 0) +
                                COALESCE("% Actively eroding: Hagg/gully", 0) +
                                COALESCE("% Drained: Hagg/gully", 0)
                            ) / 100.0
                        ) +
                        (-1) * "difference" * (
                            (
                                COALESCE("% Modified", 0) +
                                COALESCE("% Drained: Artificial", 0) +
                                COALESCE("% Drained", 0) +
                                COALESCE("% Near natural", 0) +
                                COALESCE("% Not provided", 0) +
                                COALESCE("% No peat", 0) +
                                COALESCE("% NA", 0)
                            ) / 100.0
                        ) +
                        (-1) * "difference" * (
                            COALESCE("% Forested / previously forested", 0) / 100.0
                        )
                    )
                ) * 0.85
            )
        ELSE 0
    END AS "Unspecified assigned to Modified",
    
    -- Unspecified assigned to Actively Eroding
    CASE 
        WHEN forestry = FALSE THEN 
            (
                (
                    (-1) * "difference" 
                    - (
                        (-1) * "difference" * (
                            (
                                COALESCE("% Actively eroding: Flat bare", 0) +
                                COALESCE("% Actively eroding", 0) +
                                COALESCE("% Actively eroding: Hagg/gully", 0) +
                                COALESCE("% Drained: Hagg/gully", 0)
                            ) / 100.0
                        ) +
                        (-1) * "difference" * (
                            (
                                COALESCE("% Modified", 0) +
                                COALESCE("% Drained: Artificial", 0) +
                                COALESCE("% Drained", 0) +
                                COALESCE("% Near natural", 0) +
                                COALESCE("% Not provided", 0) +
                                COALESCE("% No peat", 0) +
                                COALESCE("% NA", 0)
                            ) / 100.0
                        ) +
                        (-1) * "difference" * (
                            COALESCE("% Forested / previously forested", 0) / 100.0
                        )
                    )
                ) * 0.15
            )
        ELSE 0
    END AS "Unspecified assigned to Actively Eroding"

FROM pa_ghg_reporting.ghg_report_2026_X; ------ REPLACE THE NEW TABLE HERE!!!!
-- =====================================================================


DO $$
DECLARE
    ts TEXT := to_char(NOW(), 'YYYYMMDD_HH24MISS');

    second_tbl TEXT := format('second_rewetted_summary_table_%s', ts);
    third_tbl  TEXT := format('third_ghg_report_2026_summary_%s', ts);
    ef_tbl     TEXT := format('emission_factors_final_calcs_%s', ts);
BEGIN

    --------------------------------------------------------------------
    -- 1) SECOND TABLE: grant-level summary (m²), timestamped name
    --------------------------------------------------------------------
    EXECUTE format($sql$
        DROP TABLE IF EXISTS pa_ghg_reporting.%I;
        CREATE TABLE pa_ghg_reporting.%I AS
        SELECT 
            "grant_id",
            
            -- Woodland
            SUM(
                COALESCE("Broadleaved - Broadleaved - Forest", 0) +
                COALESCE("Conifer - Conifer - Forest", 0) +
                COALESCE("Scrub - Scrub - Forest", 0) +
                COALESCE("Woodland - Mixed or unknown - Forest", 0) +
                COALESCE("Unspecified assigned to Forest", 0)
            ) AS "Woodland",
            
            -- Cropland
            SUM(
                COALESCE("Cropland - Arable - Cropland", 0)
            ) AS "Cropland",
            
            -- Eroding
            SUM(
                COALESCE("Eroding - Eroding - Eroded" * 0.15, 0) +
                COALESCE("Actively eroding outside peatmap", 0) +
                COALESCE("Unspecified assigned to Actively Eroding", 0)
            ) AS "Eroding",
            
            -- Drained Modified Bog  (fixed logic: eroding * 0.85 + all modified bits)
            SUM(
                COALESCE("Eroding - Eroding - Eroded", 0) * 0.85 +
                COALESCE("Modified - Bracken-dominated - Modified Bog (LCA Uplands Correc", 0) +
                COALESCE("Mapping offset - Mapping offset - Near Natural Bog", 0) +
                COALESCE("Grassland - Extensive grassland - Modified Bog (LCA Uplands Cor", 0) +
                COALESCE("Modified - Heather-dominated - Modified Bog", 0) +
                COALESCE("Modified - Molinia-dominated - Modified Bog", 0) +
                COALESCE("Other - No cover data - Near Natural Bog", 0) +
                COALESCE("Semi-natural - Near natural - Near Natural Bog", 0) +
                COALESCE("Settlement - Settlement - Settlement", 0) +
                COALESCE("Modified outside peat map", 0) +
                COALESCE("Unspecified assigned to Modified", 0)
            ) AS "Drained Modified Bog",
            
            -- Intensive Grassland
            SUM(
                COALESCE("Grassland - Intensive grassland - Intensive Grassland", 0)
            ) AS "Intensive Grassland",
            
            -- Extensive Grassland
            SUM(
                COALESCE("Grassland - Extensive grassland - Extensive Grassland", 0)
            ) AS "Extensive Grassland",
            
            -- Industrial Peat Extraction
            SUM(
                COALESCE("Peat extraction - Industrial - Industrial Extraction", 0)
            ) AS "Industrial Peat Extraction",
            
            -- Domestic Extraction
            SUM(
                COALESCE("Peat extraction - Domestic or unknown - Domestic Extraction", 0)
            ) AS "Domestic Extraction",
            
            -- Project Area (sum of all)
            SUM(
                COALESCE("Broadleaved - Broadleaved - Forest", 0) +
                COALESCE("Conifer - Conifer - Forest", 0) +
                COALESCE("Scrub - Scrub - Forest", 0) +
                COALESCE("Woodland - Mixed or unknown - Forest", 0) +
                COALESCE("Unspecified assigned to Forest", 0) +
                COALESCE("Cropland - Arable - Cropland", 0) +
                COALESCE("Eroding - Eroding - Eroded" * 0.15, 0) +
                COALESCE("Actively eroding outside peatmap", 0) +
                COALESCE("Unspecified assigned to Actively Eroding", 0) +
                COALESCE("Eroding - Eroding - Eroded", 0) * 0.85 +
                COALESCE("Grassland - Extensive grassland - Modified Bog (LCA Uplands Cor", 0) +
                COALESCE("Mapping offset - Mapping offset - Near Natural Bog", 0) +
                COALESCE("Modified - Bracken-dominated - Modified Bog (LCA Uplands Correc", 0) +
                COALESCE("Modified - Heather-dominated - Modified Bog", 0) +
                COALESCE("Modified - Molinia-dominated - Modified Bog", 0) +
                COALESCE("Other - No cover data - Near Natural Bog", 0) +
                COALESCE("Semi-natural - Near natural - Near Natural Bog", 0) +
                COALESCE("Settlement - Settlement - Settlement", 0) +
                COALESCE("Modified outside peat map", 0) +
                COALESCE("Unspecified assigned to Modified", 0) +
                COALESCE("Grassland - Intensive grassland - Intensive Grassland", 0) +
                COALESCE("Grassland - Extensive grassland - Extensive Grassland", 0) +
                COALESCE("Peat extraction - Industrial - Industrial Extraction", 0) +
                COALESCE("Peat extraction - Domestic or unknown - Domestic Extraction", 0)
            ) AS "Project Area"

        FROM pa_ghg_reporting.first_ghg_report_2026_with_unespecied
        GROUP BY "grant_id"
        ORDER BY "grant_id";
    $sql$, second_tbl, second_tbl);

    -- Convenience view pointing to the latest grant-level m² summary
	DROP VIEW IF EXISTS pa_ghg_reporting.second_rewetted_summary_table_latest;
    EXECUTE format($v$
        CREATE OR REPLACE VIEW pa_ghg_reporting.second_rewetted_summary_table_latest AS
        SELECT * FROM pa_ghg_reporting.%I;
    $v$, second_tbl);


    --------------------------------------------------------------------
    -- 2) THIRD TABLE: grant-level summary in hectares, timestamped name
    --------------------------------------------------------------------
    EXECUTE format($sql$
        DROP TABLE IF EXISTS pa_ghg_reporting.%I;
        CREATE TABLE pa_ghg_reporting.%I AS
        SELECT 
            "grant_id",
            ROUND(SUM("Woodland")::numeric / 10000, 0)              AS "Woodland (ha)",
            ROUND(SUM("Cropland")::numeric / 10000, 0)              AS "Cropland (ha)",
            ROUND(SUM("Eroding")::numeric / 10000, 0)               AS "Eroding (ha)",
            ROUND(SUM("Drained Modified Bog")::numeric / 10000, 0)  AS "Drained Modified Bog (ha)",
            ROUND(SUM("Intensive Grassland")::numeric / 10000, 0)   AS "Intensive Grassland (ha)",
            ROUND(SUM("Extensive Grassland")::numeric / 10000, 0)   AS "Extensive Grassland (ha)",
            ROUND(SUM("Industrial Peat Extraction")::numeric / 10000, 0) AS "Industrial Peat Extraction (ha)",
            ROUND(SUM("Domestic Extraction")::numeric / 10000, 0)   AS "Domestic Extraction (ha)",
            ROUND(SUM("Project Area")::numeric / 10000, 0)          AS "Project Area (ha)"
        FROM pa_ghg_reporting.%I
        GROUP BY "grant_id"
        ORDER BY "grant_id";
    $sql$, third_tbl, third_tbl, second_tbl);

    -- Convenience view pointing to the latest grant-level ha summary
    EXECUTE format($v$
        CREATE OR REPLACE VIEW pa_ghg_reporting.third_ghg_report_2026_summary_latest AS
        SELECT * FROM pa_ghg_reporting.%I;
    $v$, third_tbl);


    --------------------------------------------------------------------
    -- 3) EMISSION FACTOR CALCS: by grant_id, timestamped name
    --------------------------------------------------------------------
    EXECUTE format($sql$
        DROP TABLE IF EXISTS pa_ghg_reporting.%I;
        CREATE TABLE pa_ghg_reporting.%I AS
        WITH src AS (
            SELECT * FROM pa_ghg_reporting.%I
        ),
        unpivoted AS (
            SELECT "grant_id", 'Woodland (ha)'                 AS category, "Woodland (ha)"                 AS area FROM src
            UNION ALL SELECT "grant_id", 'Cropland (ha)',              "Cropland (ha)"              FROM src
            UNION ALL SELECT "grant_id", 'Eroding (ha)',               "Eroding (ha)"               FROM src
            UNION ALL SELECT "grant_id", 'Drained Modified Bog (ha)',  "Drained Modified Bog (ha)"  FROM src
            UNION ALL SELECT "grant_id", 'Intensive Grassland (ha)',   "Intensive Grassland (ha)"   FROM src
            UNION ALL SELECT "grant_id", 'Extensive Grassland (ha)',   "Extensive Grassland (ha)"   FROM src
            UNION ALL SELECT "grant_id", 'Industrial Peat Extraction (ha)', "Industrial Peat Extraction (ha)" FROM src
            UNION ALL SELECT "grant_id", 'Domestic Extraction (ha)',   "Domestic Extraction (ha)"   FROM src
        ),
        joined AS (
            SELECT
                u."grant_id",
                m.emission_factor_pre,
                m.emission_factor_post,
                u.area,
                pre_factors.total_break1  AS pre_break1,
                pre_factors.total_break2  AS pre_break2,
                post_factors.total_break1 AS post_break1,
                post_factors.total_break2 AS post_break2,
                u.area * pre_factors.total_break1::NUMERIC  AS pre_emissions_break1,
                u.area * pre_factors.total_break2::NUMERIC  AS pre_emissions_break2,
                u.area * post_factors.total_break1::NUMERIC AS post_emissions_break1,
                u.area * post_factors.total_break2::NUMERIC AS post_emissions_break2
            FROM unpivoted u
            JOIN pa_ghg_reporting.emissions_factors_mapping_table m
              ON u.category = m.category
            JOIN pa_ghg_reporting.emission_factors_table pre_factors
              ON lower(btrim(pre_factors.peat_condition)) = lower(btrim(m.emission_factor_pre))
            JOIN pa_ghg_reporting.emission_factors_table post_factors
              ON lower(btrim(post_factors.peat_condition)) = lower(btrim(
                   CASE
                     WHEN m.category = 'Drained Modified Bog (ha)'
                      AND m.emission_factor_post = 'Rewetted Modified Bog'
                     THEN 'Rewetted Modified (Semi-natural) Bog'
                     ELSE m.emission_factor_post
                   END
                 ))
        )
        SELECT
            "grant_id",
            emission_factor_pre,
            emission_factor_post,
            SUM(pre_emissions_break1)  AS pre_emissions_break1,
            SUM(pre_emissions_break2)  AS pre_emissions_break2,
            SUM(post_emissions_break1) AS post_emissions_break1,
            SUM(post_emissions_break2) AS post_emissions_break2,
            SUM(pre_emissions_break1) - SUM(post_emissions_break1) AS diff_break1,
            SUM(pre_emissions_break2) - SUM(post_emissions_break2) AS diff_break2
        FROM joined
        GROUP BY "grant_id", emission_factor_pre, emission_factor_post
        ORDER BY "grant_id", emission_factor_pre;
    $sql$, ef_tbl, ef_tbl, third_tbl);

    -- Convenience view pointing to the latest emission factor calcs.
    -- Adds delivery_partner, max financial_year_end, and forestry using exact comma-separated grant_id tokens.
    -- Matching priority: use the first grant_id in a.grant_id that has a match;
    -- if it has no match, try the second, then the third, etc.
    EXECUTE format($v$
        CREATE OR REPLACE VIEW pa_ghg_reporting.emission_factors_final_calcs_latest AS
        WITH
        b_split AS (
            SELECT
                btrim(x.grant_id_part) AS grant_id_part,
                max(b.delivery_partner) AS delivery_partner,
                max(b.financial_year_end) AS financial_year_end
            FROM pa_reporting.reported_ha b
            CROSS JOIN LATERAL regexp_split_to_table(b.grant_id::text, '\s*,\s*') AS x(grant_id_part)
            WHERE btrim(x.grant_id_part) <> ''
            GROUP BY btrim(x.grant_id_part)
        ),
        c_split AS (
            SELECT
                btrim(x.grant_id_part) AS grant_id_part,
                bool_or(c.forestry) AS forestry
            FROM pa_ghg_reporting.ghg_report_2026_X c  ------ REPLACE THE NEW TABLE HERE!!!!
            CROSS JOIN LATERAL regexp_split_to_table(c.grant_id::text, '\s*,\s*') AS x(grant_id_part)
            WHERE btrim(x.grant_id_part) <> ''
            GROUP BY btrim(x.grant_id_part)
        )
        SELECT
            a.grant_id,
            a.emission_factor_pre,
            a.emission_factor_post,
            a.pre_emissions_break1,
            a.pre_emissions_break2,
            a.post_emissions_break1,
            a.post_emissions_break2,
            a.diff_break1,
            a.diff_break2,
            b.delivery_partner,
            c.forestry,
            b.financial_year_end
        FROM pa_ghg_reporting.%I a
        LEFT JOIN LATERAL (
            SELECT
                bs.delivery_partner,
                bs.financial_year_end
            FROM regexp_split_to_table(a.grant_id::text, '\s*,\s*') WITH ORDINALITY AS ag(grant_id_part, grant_order)
            JOIN b_split bs
              ON btrim(ag.grant_id_part) = bs.grant_id_part
            WHERE btrim(ag.grant_id_part) <> ''
            ORDER BY ag.grant_order
            LIMIT 1
        ) b ON TRUE
        LEFT JOIN LATERAL (
            SELECT cs.forestry
            FROM regexp_split_to_table(a.grant_id::text, '\s*,\s*') WITH ORDINALITY AS ag(grant_id_part, grant_order)
            JOIN c_split cs
              ON btrim(ag.grant_id_part) = cs.grant_id_part
            WHERE btrim(ag.grant_id_part) <> ''
            ORDER BY ag.grant_order
            LIMIT 1
        ) c ON TRUE;
    $v$, ef_tbl);

END$$;


---------------------------- Calculate emissions savings (by grant_id)

SELECT 
    grant_id, 
    SUM(pre_emissions_break1)  AS pre_emission_break1,
    SUM(pre_emissions_break2)  AS pre_emission_break2,
    SUM(post_emissions_break1) AS post_emission_break1,
    SUM(post_emissions_break2) AS post_emission_break2,
    SUM(diff_break1)           AS savings_break1,
    SUM(diff_break2)           AS savings_break2
FROM pa_ghg_reporting.emission_factors_final_calcs_latest
GROUP BY grant_id
ORDER BY grant_id;


SELECT * FROM pa_ghg_reporting.emission_factors_final_calcs_latest;
