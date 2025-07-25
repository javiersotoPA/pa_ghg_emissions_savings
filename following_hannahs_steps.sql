DROP TABLE IF EXISTS pa_ghg_reporting.first_ghg_report_2025_with_unespecied;
CREATE TABLE pa_ghg_reporting.first_ghg_report_2025_with_unespecied AS
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

FROM pa_ghg_reporting.ghg_report_2025;


-- Generate second table called reweted summary table using first_ghg_report_2025_with_unespecified

DROP TABLE IF EXISTS pa_ghg_reporting.second_rewetted_summary_table; 
CREATE TABLE pa_ghg_reporting.second_rewetted_summary_table AS
SELECT 
    "financial_year_end" AS "Year",
    
    -- Woodland
    COALESCE("Broadleaved - Broadleaved - Forest", 0) +
    COALESCE("Conifer - Conifer - Forest", 0) +
    COALESCE("Scrub - Scrub - Forest", 0) +
    COALESCE("Woodland - Mixed or unknown - Forest", 0) +
    COALESCE("Unspecified assigned to Forest", 0) AS "Woodland",
    
    -- Cropland
    COALESCE("Cropland - Arable - Cropland", 0) AS "Cropland",
    
    -- Eroding
    COALESCE("Eroding - Eroding - Eroded"*0.15, 0) +
    COALESCE("Actively eroding outside peatmap", 0) +
    COALESCE("Unspecified assigned to Actively Eroding", 0) AS "Eroding",
    
    -- Drained Modified Bog
    COALESCE("Eroding - Eroding - Eroded"*0.85, 0+
    COALESCE("Modified - Bracken-dominated - Modified Bog (LCA Uplands Correc", 0) +
    COALESCE("Mapping offset - Mapping offset - Near Natural Bog", 0) +
    COALESCE("Grassland - Extensive grassland - Modified Bog (LCA Uplands Cor", 0) +
    COALESCE("Modified - Heather-dominated - Modified Bog", 0) +
    COALESCE("Modified - Molinia-dominated - Modified Bog", 0) +
    COALESCE("Other - No cover data - Near Natural Bog", 0) +
    COALESCE("Semi-natural - Near natural - Near Natural Bog", 0) +
    COALESCE("Settlement - Settlement - Settlement", 0) +
    COALESCE("Modified outside peat map", 0) +
    COALESCE("Unspecified assigned to Modified", 0)) AS "Drained Modified Bog",
    
    -- Intensive Grassland
    COALESCE("Grassland - Intensive grassland - Intensive Grassland", 0) AS "Intensive Grassland",
    
    -- Extensive Grassland
    COALESCE("Grassland - Extensive grassland - Extensive Grassland", 0) AS "Extensive Grassland",
    
    -- Industrial Peat Extraction
    COALESCE("Peat extraction - Industrial - Industrial Extraction", 0) AS "Industrial Peat Extraction",
    
    -- Domestic Extraction
    COALESCE("Peat extraction - Domestic or unknown - Domestic Extraction", 0) AS "Domestic Extraction",
    
    -- Project Area (sum of all)
    (
        COALESCE("Broadleaved - Broadleaved - Forest", 0) +
        COALESCE("Conifer - Conifer - Forest", 0) +
        COALESCE("Scrub - Scrub - Forest", 0) +
        COALESCE("Woodland - Mixed or unknown - Forest", 0) +
        COALESCE("Unspecified assigned to Forest", 0) +
        COALESCE("Cropland - Arable - Cropland", 0) +
        COALESCE("Eroding - Eroding - Eroded"*0.15, 0) +
        COALESCE("Actively eroding outside peatmap", 0) +
        COALESCE("Unspecified assigned to Actively Eroding", 0) +
        COALESCE("Eroding - Eroding - Eroded"*0.85, 0) +
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

FROM pa_ghg_reporting.first_ghg_report_2025_with_unespecied;
				 
-- Group the summary table by year:

DROP TABLE IF EXISTS pa_ghg_reporting.third_ghg_report_2025_summary;
CREATE TABLE pa_ghg_reporting.third_ghg_report_2025_summary AS
SELECT 
    "Year",
    ROUND(SUM("Woodland")::numeric / 10000, 0) AS "Woodland (ha)",
    ROUND(SUM("Cropland")::numeric / 10000, 0) AS "Cropland (ha)",
    ROUND(SUM("Eroding")::numeric / 10000, 0) AS "Eroding (ha)",
    ROUND(SUM("Drained Modified Bog")::numeric / 10000, 0) AS "Drained Modified Bog (ha)",
    ROUND(SUM("Intensive Grassland")::numeric / 10000, 0) AS "Intensive Grassland (ha)",
    ROUND(SUM("Extensive Grassland")::numeric / 10000, 0) AS "Extensive Grassland (ha)",
    ROUND(SUM("Industrial Peat Extraction")::numeric / 10000, 0) AS "Industrial Peat Extraction (ha)",
    ROUND(SUM("Domestic Extraction")::numeric / 10000, 0) AS "Domestic Extraction (ha)",
    ROUND(SUM("Project Area")::numeric / 10000, 0) AS "Project Area (ha)"
FROM pa_ghg_reporting.second_rewetted_summary_table
GROUP BY "Year"
ORDER BY "Year";

-- Drop existing final calcs table if it exists
DROP TABLE IF EXISTS pa_ghg_reporting.emission_factors_final_calcs;

-- Create the updated table with post and diff columns
CREATE TABLE pa_ghg_reporting.emission_factors_final_calcs (
  year INT,
  emission_factor_pre VARCHAR(255),
  emission_factor_post VARCHAR(255),
  pre_emissions_break1 NUMERIC,
  pre_emissions_break2 NUMERIC,
  post_emissions_break1 NUMERIC,
  post_emissions_break2 NUMERIC,
  diff_break1 NUMERIC,
  diff_break2 NUMERIC
);

-- Main logic
WITH unpivoted AS (
    SELECT "Year", 'Woodland (ha)' AS category, "Woodland (ha)" AS area FROM pa_ghg_reporting.third_ghg_report_2025_summary
    UNION ALL SELECT "Year", 'Cropland (ha)', "Cropland (ha)" FROM pa_ghg_reporting.third_ghg_report_2025_summary
    UNION ALL SELECT "Year", 'Eroding (ha)', "Eroding (ha)" FROM pa_ghg_reporting.third_ghg_report_2025_summary
    UNION ALL SELECT "Year", 'Drained Modified Bog (ha)', "Drained Modified Bog (ha)" FROM pa_ghg_reporting.third_ghg_report_2025_summary
    UNION ALL SELECT "Year", 'Intensive Grassland (ha)', "Intensive Grassland (ha)" FROM pa_ghg_reporting.third_ghg_report_2025_summary
    UNION ALL SELECT "Year", 'Extensive Grassland (ha)', "Extensive Grassland (ha)" FROM pa_ghg_reporting.third_ghg_report_2025_summary
    UNION ALL SELECT "Year", 'Industrial Peat Extraction (ha)', "Industrial Peat Extraction (ha)" FROM pa_ghg_reporting.third_ghg_report_2025_summary
    UNION ALL SELECT "Year", 'Domestic Extraction (ha)', "Domestic Extraction (ha)" FROM pa_ghg_reporting.third_ghg_report_2025_summary
),

joined AS (
    SELECT
        u."Year",
        m.emission_factor_pre,
        m.emission_factor_post,
        u.area,
        pre_factors.total_break1 AS pre_break1,
        pre_factors.total_break2 AS pre_break2,
        post_factors.total_break1 AS post_break1,
        post_factors.total_break2 AS post_break2,
        u.area * pre_factors.total_break1::NUMERIC AS pre_emissions_break1,
        u.area * pre_factors.total_break2::NUMERIC AS pre_emissions_break2,
        u.area * post_factors.total_break1::NUMERIC AS post_emissions_break1,
        u.area * post_factors.total_break2::NUMERIC AS post_emissions_break2
    FROM unpivoted u
    JOIN pa_ghg_reporting.emissions_factors_mapping_table m ON u.category = m.category
    JOIN pa_ghg_reporting.emission_factors_table pre_factors ON m.emission_factor_pre = pre_factors.peat_condition
    JOIN pa_ghg_reporting.emission_factors_table post_factors ON m.emission_factor_post = post_factors.peat_condition
)

-- Final INSERT with calculated differences
INSERT INTO pa_ghg_reporting.emission_factors_final_calcs
SELECT
    "Year",
    emission_factor_pre,
    emission_factor_post,
    SUM(pre_emissions_break1) AS pre_emissions_break1,
    SUM(pre_emissions_break2) AS pre_emissions_break2,
    SUM(post_emissions_break1) AS post_emissions_break1,
    SUM(post_emissions_break2) AS post_emissions_break2,
    SUM(pre_emissions_break1) - SUM(post_emissions_break1) AS diff_break1,
    SUM(pre_emissions_break2) - SUM(post_emissions_break2) AS diff_break2
FROM joined
GROUP BY "Year", emission_factor_pre, emission_factor_post
ORDER BY "Year", emission_factor_pre;



---------------------------- Calculate emissions savings

SELECT year, 
sum(pre_emissions_break1) as pre_emission_break1,
sum(pre_emissions_break2) as pre_emission_break2,
sum(post_emissions_break1) as post_emission_break1,
sum(post_emissions_break2) as post_emission_break2,
sum(diff_break1) as savings_break1,
sum(diff_break2) as savings_break2
 FROM pa_ghg_reporting.emission_factors_final_calcs
 group by year
 order by year













