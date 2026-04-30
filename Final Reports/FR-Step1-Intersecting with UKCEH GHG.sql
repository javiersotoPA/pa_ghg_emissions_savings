-- =========================================================
-- BLOCK 0 — SETUP (no business logic changes)
-- - Creates temp schema if missing
-- - Defines run suffix YYYYMMDD
-- =========================================================

DO $$
BEGIN
  RAISE NOTICE 'BLOCK 0 started: Setup (schemas + run suffix YYYYMMDD).';
END $$;

CREATE SCHEMA IF NOT EXISTS temp;

-- Run suffix based on execution date (YYYYMMDD)
-- We'll use this later to create the final table name.
DO $$
BEGIN
  RAISE NOTICE 'Run date suffix will be: %', to_char(CURRENT_DATE, 'YYYYMMDD');
END $$;

DO $$
BEGIN
  RAISE NOTICE 'BLOCK 0 completed: Setup done.';
END $$;


-- =========================================================
-- BLOCK A — Build base geometries
-- - Create base polygon set (from multiple sources)
-- - Insert centroid buffers where polygons don't exist
-- - Convert MultiPolygon -> Polygon (dump) and add FY end
-- =========================================================

DO $$
BEGIN
  RAISE NOTICE 'BLOCK A started: Build base geometries (polygons + centroid buffers + polygon normalization).';
END $$;

-- A1) Drop & create base working table (moved to temp schema)
DROP TABLE IF EXISTS temp.ghg_report_temp;

CREATE TABLE temp.ghg_report_temp AS
SELECT DISTINCT foo.grant_id, foo.geom
FROM (
  WITH all_polygons AS (
    SELECT subsite_boundaries.project_id AS grant_id,
           subsite_boundaries.geom
    FROM old_restoration_data_model.subsite_boundaries
    WHERE NOT (subsite_boundaries.project_id::text IN (
      SELECT DISTINCT restoration_footprint_30.grant_id
      FROM pa_final_report.restoration_footprint_30
    ))
    UNION
    SELECT restoration_footprint_30.grant_id,
           restoration_footprint_30.geom
    FROM pa_final_report.restoration_footprint_30
    UNION
    SELECT pa_project_boundaries.grant_ref::text AS grant_id,
           pa_project_boundaries.wkb_geometry AS geom
    FROM pa_legacy_data.pa_project_boundaries
    WHERE NOT (pa_project_boundaries.grant_ref::text IN (
      SELECT DISTINCT restoration_footprint_30.grant_id
      FROM pa_final_report.restoration_footprint_30
      UNION
      SELECT DISTINCT subsite_boundaries.project_id AS grant_id
      FROM old_restoration_data_model.subsite_boundaries
    ))
  )
  SELECT row_number() OVER () AS id,
         a.grant_id,
         a.geom,
         b.financial_year_end,
         b.hectares AS reported_hectares,
         round((st_area(a.geom) / 10000::double precision)::numeric, 2) AS hectares_subsite,
         now() AS last_reviewed
  FROM all_polygons a
  JOIN pa_reporting.reported_ha b ON a.grant_id::text = b.grant_id
) AS foo;

-- A2) Insert buffered centroid-based geometries when polygon does not exist (dissolved by grant_id)
INSERT INTO temp.ghg_report_temp (grant_id, geom)
SELECT foo.grant_reference AS grant_id,
       (st_dump(st_union(geometry))).geom AS geom
FROM (
  SELECT row_number() OVER (ORDER BY site_summary_2021.grant_reference) AS id,
         site_summary_2021.grant_reference,
         site_summary_2021.project_name,
         site_summary_2021.site_name,
         site_summary_2021.financial_year_of_application,
         site_summary_2021.ha_reported_total,
         site_summary_2021.ha_reported_1213,
         site_summary_2021.ha_reported_1314,
         site_summary_2021.ha_reported_1415,
         site_summary_2021.ha_reported_1516,
         site_summary_2021.ha_reported_1617,
         site_summary_2021.ha_reported_1718,
         site_summary_2021.ha_reported_1819,
         site_summary_2021.ha_reported_1920,
         site_summary_2021.ha_reported_2021,
         st_buffer(
           site_summary_2021.geom,
           sqrt(site_summary_2021.ha_reported_total * 10000::double precision / 3.14::double precision)
         ) AS geometry,
         sqrt(site_summary_2021.ha_reported_total * 10000::double precision / 3.14::double precision) AS radius_meters
  FROM site_summary_2021
  WHERE site_summary_2021.project_type ~~* '%restoration%'::text
    AND NOT (site_summary_2021.grant_reference IN (
      SELECT DISTINCT grant_id FROM temp.ghg_report_temp
    ))
    AND site_summary_2021.project_status ~~* '%completed%'::text
    AND site_summary_2021.ha_reported_total > 0::double precision
) AS foo
GROUP BY grant_reference;

-- A3) MultiPolygon -> Polygon (dump) into temp.ghg_report_temp_poly
DROP TABLE IF EXISTS temp.ghg_report_temp_poly;

CREATE TABLE IF NOT EXISTS temp.ghg_report_temp_poly AS
SELECT row_number() OVER (PARTITION BY geom) AS id,
       grant_id,
       st_makevalid((st_dump(geom)).geom::geometry(Polygon,27700)) AS geom
FROM temp.ghg_report_temp;

-- Keep same id-reset logic as original script
CREATE SEQUENCE temp.id_seq_poly;
UPDATE temp.ghg_report_temp_poly
SET id = nextval('temp.id_seq_poly');
DROP SEQUENCE temp.id_seq_poly;

-- A4) Add financial_year_end and populate it
ALTER TABLE temp.ghg_report_temp_poly ADD COLUMN financial_year_end int;

UPDATE temp.ghg_report_temp_poly a
SET financial_year_end = b.financial_year_end
FROM (
  SELECT grant_id, max(financial_year_end) AS financial_year_end
  FROM pa_reporting.reported_ha
  GROUP BY grant_id
  ORDER BY grant_id
) b
WHERE a.grant_id = b.grant_id;

-- A5) Delete polygons fully covered by other polygons (same as original)
DELETE FROM temp.ghg_report_temp_poly
WHERE id IN (
  SELECT a.id
  FROM temp.ghg_report_temp_poly a
  JOIN temp.ghg_report_temp_poly b
    ON ST_CoveredBy(a.geom, b.geom) AND a.id != b.id
);

DO $$
BEGIN
  RAISE NOTICE 'BLOCK A completed: Base geometries ready in temp.ghg_report_temp_poly.';
END $$;


-- =========================================================
-- BLOCK B — Dissolve by Financial Year End + resolve overlaps
-- Output of this block: temp.ghg_report_2025_work  (NOT final)
-- Intermediate tables (temp schema) will be dropped later.
-- =========================================================

DO $$
BEGIN
  RAISE NOTICE 'BLOCK B started: Dissolve by FY + resolve overlaps (prioritise higher FY).';
END $$;

-- B1) Flag overlapping polygons (same FY) in temp.ghg_report_temp_poly
ALTER TABLE temp.ghg_report_temp_poly
  ADD COLUMN IF NOT EXISTS overlap smallint;

-- Step 1: overlap = 0 where there are no overlaps with any polygon
UPDATE temp.ghg_report_temp_poly a
SET overlap = 0
WHERE NOT EXISTS (
  SELECT 1
  FROM temp.ghg_report_temp_poly b
  WHERE ST_Overlaps(a.geom, b.geom)
);

-- Step 2: overlap = 1 where a polygon overlaps and FY matches
UPDATE temp.ghg_report_temp_poly a
SET overlap = 1
WHERE EXISTS (
  SELECT 1
  FROM temp.ghg_report_temp_poly b
  WHERE ST_Overlaps(a.geom, b.geom)
    AND a.financial_year_end = b.financial_year_end
);

-- B2) Dissolve overlapping polygons that share the same FY (and collect grant_ids)
DROP TABLE IF EXISTS temp.ghg_report_temp_poly_by_year;

CREATE TABLE temp.ghg_report_temp_poly_by_year AS (
  SELECT
    (st_dump(st_union(geom))).geom AS geom,
    financial_year_end,
    array_agg(grant_id) AS grant_id
  FROM temp.ghg_report_temp_poly
  WHERE overlap = 1
  GROUP BY financial_year_end
);

-- B3) Bring grant_ids back cleanly using centroids-in-polygon
DROP TABLE IF EXISTS temp.ghg_report_temp_poly_by_year_grants;

CREATE TABLE temp.ghg_report_temp_poly_by_year_grants AS (
  SELECT array_agg(DISTINCT a.grant_id)::varchar AS grant_id,
         b.financial_year_end,
         b.geom
  FROM (SELECT grant_id, st_pointonsurface(geom) AS geom
        FROM temp.ghg_report_temp_poly
        WHERE overlap = 1) a
  LEFT JOIN temp.ghg_report_temp_poly_by_year b
    ON (st_intersects(a.geom, b.geom))
  GROUP BY b.financial_year_end, b.geom
);

-- Insert polygons that do NOT overlap (plus null overlap just in case)
INSERT INTO temp.ghg_report_temp_poly_by_year_grants (grant_id, financial_year_end, geom)
SELECT grant_id, financial_year_end, geom
FROM temp.ghg_report_temp_poly
WHERE overlap != 1;

INSERT INTO temp.ghg_report_temp_poly_by_year_grants (grant_id, financial_year_end, geom)
SELECT grant_id, financial_year_end, geom
FROM temp.ghg_report_temp_poly
WHERE overlap IS NULL;

-- Replace the dissolved table with the “grants-fixed” one
DROP TABLE IF EXISTS temp.ghg_report_temp_poly_by_year;
ALTER TABLE temp.ghg_report_temp_poly_by_year_grants
  RENAME TO ghg_report_temp_poly_by_year;

-- Remove { } introduced by array cast
UPDATE temp.ghg_report_temp_poly_by_year
SET grant_id = REPLACE(grant_id, '{', '')
WHERE grant_id LIKE '{%}';

UPDATE temp.ghg_report_temp_poly_by_year
SET grant_id = REPLACE(grant_id, '}', '')
WHERE grant_id LIKE '%}';

-- B4) Resolve overlapping issues (prioritise polygons with higher FY)
ALTER TABLE temp.ghg_report_temp_poly_by_year
  ADD COLUMN IF NOT EXISTS overlap smallint;

-- Step 1: overlap = 0 where there are no overlaps
UPDATE temp.ghg_report_temp_poly_by_year a
SET overlap = 0
WHERE NOT EXISTS (
  SELECT 1
  FROM temp.ghg_report_temp_poly_by_year b
  WHERE ST_Overlaps(a.geom, b.geom)
);

-- Step 2: overlap = 1 where overlaps exist
UPDATE temp.ghg_report_temp_poly_by_year a
SET overlap = 1
WHERE EXISTS (
  SELECT 1
  FROM temp.ghg_report_temp_poly_by_year b
  WHERE ST_Overlaps(a.geom, b.geom)
);

-- Indexes (same intent as original)
CREATE INDEX IF NOT EXISTS idx_ghg_report_temp_poly_by_year_geom
  ON temp.ghg_report_temp_poly_by_year USING gist (geom);

CREATE INDEX IF NOT EXISTS idx_ghg_report_temp_poly_by_year
  ON temp.ghg_report_temp_poly_by_year(financial_year_end);

CREATE INDEX IF NOT EXISTS idx_ghg_report_overlap_by_year
  ON temp.ghg_report_temp_poly_by_year(overlap);

CREATE INDEX IF NOT EXISTS idx_ghg_report_grant_by_year
  ON temp.ghg_report_temp_poly_by_year(grant_id);

-- Create overlap-resolved polygons by subtracting higher-FY geometry from lower-FY geometry
DROP TABLE IF EXISTS temp.ghg_report_resolve_overlap;

CREATE TABLE temp.ghg_report_resolve_overlap AS (
  WITH just_overlapping_ones AS (
    SELECT *
    FROM temp.ghg_report_temp_poly_by_year
    WHERE overlap = 1
  )
  SELECT
    grant_id,
    financial_year_end,
    ST_Multi(COALESCE(
      ST_Difference(a.geom, blade.geom),
      a.geom
    )) AS geom
  FROM just_overlapping_ones AS a
  CROSS JOIN LATERAL (
    SELECT ST_Union(geom) AS geom
    FROM just_overlapping_ones AS b
    WHERE a.financial_year_end < b.financial_year_end
  ) AS blade
);

-- B5) Second iteration dissolve: some outputs above will still overlap
DROP TABLE IF EXISTS temp.ghg_report_resolve_overlap_round2;

CREATE TABLE temp.ghg_report_resolve_overlap_round2 AS
SELECT
  row_number() OVER () AS id,
  string_agg(DISTINCT foo.grant_id, ',') AS grant_id,
  string_agg(DISTINCT foo.financial_year_end::text, ',') AS financial_year_end,
  foo.geom
FROM (
  SELECT b.grant_id, a.geom, b.financial_year_end
  FROM (
    SELECT (st_dump(st_union(geom))).geom AS geom
    FROM temp.ghg_report_resolve_overlap
    GROUP BY financial_year_end
  ) a
  JOIN (
    SELECT financial_year_end, grant_id, ST_PointOnSurface(geom) AS geom
    FROM temp.ghg_report_resolve_overlap
  ) b
  ON st_within(b.geom, a.geom)
) AS foo
GROUP BY financial_year_end, geom;

-- B6) Combine: overlap=1 resolved (round2) + overlap=0 untouched
DROP TABLE IF EXISTS temp.ghg_report_2025_work;

CREATE TABLE temp.ghg_report_2025_work AS
SELECT grant_id, geom FROM temp.ghg_report_resolve_overlap_round2
UNION
SELECT grant_id, geom FROM temp.ghg_report_temp_poly_by_year
WHERE overlap = 0;

DO $$
BEGIN
  RAISE NOTICE 'BLOCK B completed: Output ready in temp.ghg_report_2025_work.';
END $$;


-- =========================================================
-- BLOCK C — Baseline clip+dissolve + transpose rows->columns
-- Output of this block: temp.ghg_report_2025_draft  (NOT final)
-- =========================================================

DO $$
BEGIN
  RAISE NOTICE 'BLOCK C started: Clip baseline (external_data.ghg_20240131) to PA polygons + dissolve + transpose to wide table.';
END $$;

-- C1) Clip the baseline map to PA polygons and dissolve by lc_level2, lc_level3, condition
DROP TABLE IF EXISTS temp.ghg_20240131_clip_dissolve;

CREATE TABLE temp.ghg_20240131_clip_dissolve AS
SELECT
  row_number() OVER () AS id,
  foo.grant_id,
  foo.lc_level2,
  foo.lc_level3,
  foo.condition,
  foo.project_area,
  (st_dump(st_union(foo.geom))).geom AS geom,
  foo.geom2
FROM (
  SELECT
    row_number() OVER () AS id,
    grant_id,
    lc_level2,
    lc_level3,
    condition,
    st_intersection(ghg.geom, poly.geom) AS geom,
    round(st_area(poly.geom)::numeric, 2) AS project_area,
    poly.geom AS geom2
  FROM external_data.ghg_20240131 AS ghg,
       temp.ghg_report_2025_work AS poly
  WHERE st_intersects(ghg.geom, poly.geom)  -- clipping query
) AS foo
GROUP BY grant_id, lc_level2, lc_level3, condition, project_area, geom2;

-- C2) TRANSPOSE rows to columns (wide format)
DROP TABLE IF EXISTS temp.ghg_report_2025_draft;

CREATE TABLE temp.ghg_report_2025_draft AS
WITH conditions AS (
  SELECT
    row_number() OVER () AS id,
    grant_id,
    lc_level2 || ' - ' || lc_level3 || ' - ' || condition AS emissions_category,
    project_area,
    round(SUM(st_area(geom))::numeric, 2) AS emissions_area,
    geom2
  FROM temp.ghg_20240131_clip_dissolve
  GROUP BY grant_id, emissions_category, project_area, geom2
  ORDER BY grant_id, emissions_category
)
SELECT
  grant_id,
  project_area,

  COALESCE(max(CASE WHEN emissions_category = 'Broadleaved - Broadleaved - Forest'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Broadleaved - Broadleaved - Forest",

  COALESCE(max(CASE WHEN emissions_category = 'Conifer - Conifer - Forest'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Conifer - Conifer - Forest",

  COALESCE(max(CASE WHEN emissions_category = 'Cropland - Arable - Cropland'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Cropland - Arable - Cropland",

  COALESCE(max(CASE WHEN emissions_category = 'Eroding - Eroding - Eroded'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Eroding - Eroding - Eroded",

  COALESCE(max(CASE WHEN emissions_category = 'Grassland - Extensive grassland - Extensive Grassland'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Grassland - Extensive grassland - Extensive Grassland",

  COALESCE(max(CASE WHEN emissions_category = 'Grassland - Extensive grassland - Modified Bog (LCA Uplands Correction)'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Grassland - Extensive grassland - Modified Bog (LCA Uplands Correction)",

  COALESCE(max(CASE WHEN emissions_category = 'Grassland - Intensive grassland - Intensive Grassland'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Grassland - Intensive grassland - Intensive Grassland",

  COALESCE(max(CASE WHEN emissions_category = 'Mapping offset - Mapping offset - Near Natural Bog'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Mapping offset - Mapping offset - Near Natural Bog",

  COALESCE(max(CASE WHEN emissions_category = 'Modified - Bracken-dominated - Modified Bog (LCA Uplands Correction)'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Modified - Bracken-dominated - Modified Bog (LCA Uplands Correction)",

  COALESCE(max(CASE WHEN emissions_category = 'Modified - Heather-dominated - Modified Bog'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Modified - Heather-dominated - Modified Bog",

  COALESCE(max(CASE WHEN emissions_category = 'Modified - Molinia-dominated - Modified Bog'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Modified - Molinia-dominated - Modified Bog",

  COALESCE(max(CASE WHEN emissions_category = 'Other - No cover data - Near Natural Bog'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Other - No cover data - Near Natural Bog",

  COALESCE(max(CASE WHEN emissions_category = 'Peat extraction - Domestic or unknown - Domestic Extraction'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Peat extraction - Domestic or unknown - Domestic Extraction",

  COALESCE(max(CASE WHEN emissions_category = 'Peat extraction - Industrial - Industrial Extraction'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Peat extraction - Industrial - Industrial Extraction",

  COALESCE(max(CASE WHEN emissions_category = 'Scrub - Scrub - Forest'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Scrub - Scrub - Forest",

  COALESCE(max(CASE WHEN emissions_category = 'Semi-natural - Near natural - Near Natural Bog'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Semi-natural - Near natural - Near Natural Bog",

  COALESCE(max(CASE WHEN emissions_category = 'Settlement - Settlement - Settlement'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Settlement - Settlement - Settlement",

  COALESCE(max(CASE WHEN emissions_category = 'Woodland - Mixed or unknown - Forest'::text
    THEN emissions_area ELSE NULL::double precision END), 0::double precision) AS "Woodland - Mixed or unknown - Forest",

  geom2 AS geom
FROM conditions
GROUP BY grant_id, project_area, geom
ORDER BY grant_id;

-- C3) Bring missing geometries from temp.ghg_report_temp_poly (same idea as your script)
CREATE INDEX IF NOT EXISTS idx_ghg_report_2025_draft_geom
  ON temp.ghg_report_2025_draft USING gist (geom);

CREATE INDEX IF NOT EXISTS idx_ghg_report_temp_poly_geom
  ON temp.ghg_report_temp_poly USING gist (geom);

INSERT INTO temp.ghg_report_2025_draft (grant_id, project_area, geom)
SELECT
  a.grant_id,
  round(st_area(a.geom)::numeric, 2) AS project_area,
  a.geom
FROM temp.ghg_report_temp_poly a
WHERE NOT EXISTS (
  SELECT 1
  FROM temp.ghg_report_2025_draft b
  WHERE NOT ST_Disjoint(a.geom, b.geom)
);

DO $$
BEGIN
  RAISE NOTICE 'BLOCK C completed: Output ready in temp.ghg_report_2025_draft.';
END $$;
-- =========================================================
-- BLOCK D — Add attributes (project name, current use, condition, techniques, FY end, centroid, forestry, difference, version)
-- Input : temp.ghg_report_2025_draft
-- Output: temp.ghg_report_2025_enriched   (NOT final)
-- =========================================================

DO $$
BEGIN
  RAISE NOTICE 'BLOCK D started: Add attributes and derived fields to the draft table.';
END $$;

-- D0) Create a working copy (so we keep the draft intact if needed)
DROP TABLE IF EXISTS temp.ghg_report_2025_enriched;
CREATE TABLE temp.ghg_report_2025_enriched AS
SELECT * FROM temp.ghg_report_2025_draft;

-- D1) Add columns (same as original intent)
ALTER TABLE temp.ghg_report_2025_enriched
  ADD COLUMN IF NOT EXISTS difference decimal,
  ADD COLUMN IF NOT EXISTS project_name varchar,
  ADD COLUMN IF NOT EXISTS pa_current_use varchar,
  ADD COLUMN IF NOT EXISTS pa_condition_category varchar,
  ADD COLUMN IF NOT EXISTS techniques varchar,
  ADD COLUMN IF NOT EXISTS financial_year_end int,
  ADD COLUMN IF NOT EXISTS centroid varchar,
  ADD COLUMN IF NOT EXISTS forestry bool DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS version date;

-- D2) Insert centroid (unique id proxy)
UPDATE temp.ghg_report_2025_enriched
SET centroid = get_grid_ref_from_geom(ST_PointOnSurface(geom));

-- D3) Project name(s) from pa_metadata.grant_reference (unnest -> join -> nest back)
WITH
grant_id_unnest AS (
  SELECT centroid, unnest(string_to_array(grant_id, ',')) AS grant_id
  FROM temp.ghg_report_2025_enriched
),
join_use_grant_id AS (
  SELECT centroid, a.grant_id, b.project_name
  FROM grant_id_unnest a
  LEFT JOIN pa_metadata.grant_reference b
    ON a.grant_id = b.grant_id
),
grant_id_nest AS (
  SELECT centroid,
         string_agg(distinct grant_id, ',') AS grant_id,
         string_agg(distinct project_name, ',') AS "project_name(s)"
  FROM join_use_grant_id
  GROUP BY centroid
  ORDER BY centroid
)
UPDATE temp.ghg_report_2025_enriched a
SET project_name = (
  SELECT "project_name(s)" FROM grant_id_nest b WHERE a.centroid = b.centroid
);

-- Fallback when grant_ids has more than one site (same as original)
UPDATE temp.ghg_report_2025_enriched a
SET project_name = (
  SELECT project_name
  FROM pa_metadata.grant_reference b
  WHERE split_part(a.grant_id, ',', 1) = b.grant_id
)
WHERE project_name IS NULL;

-- D4) Current use from final report + site_summary_2021, then clean categories (same as original)
WITH current_use_all AS (
  SELECT grant_id,
         CASE
           WHEN current_use ILIKE '%Other%' THEN regexp_replace(current_use, '[^\\w\\s^,]', '', 'g') || ' - ' || notes
           ELSE regexp_replace(current_use, '[^\\w\\s^,]', '', 'g')
         END AS current_use
  FROM pa_final_report.site_outline
  UNION
  SELECT grant_reference AS grant_id, current_use_of_site
  FROM public.site_summary_2021
),
grant_id_unnest AS (
  SELECT centroid, unnest(string_to_array(grant_id, ',')) AS grant_id
  FROM temp.ghg_report_2025_enriched
),
join_use_grant_id AS (
  SELECT centroid, a.grant_id, b.current_use
  FROM grant_id_unnest a
  LEFT JOIN current_use_all b
    ON a.grant_id = b.grant_id
),
grant_id_nest AS (
  SELECT centroid,
         string_agg(distinct grant_id, ',') AS grant_id,
         string_agg(distinct current_use, ',') AS current_use
  FROM join_use_grant_id
  GROUP BY centroid
  ORDER BY centroid
)
UPDATE temp.ghg_report_2025_enriched a
SET pa_current_use = (
  SELECT current_use FROM grant_id_nest b WHERE a.centroid = b.centroid
);

-- Clean or update old categories (same statements, just pointing to temp)
UPDATE temp.ghg_report_2025_enriched SET pa_current_use = 'Forestry'
WHERE pa_current_use = '5,4';

UPDATE temp.ghg_report_2025_enriched SET pa_current_use = replace(pa_current_use,'5,4','Forestry')
WHERE pa_current_use LIKE '%5,4%';

UPDATE temp.ghg_report_2025_enriched SET pa_current_use = replace(pa_current_use,'1,3,4','')
WHERE pa_current_use LIKE '%1,3,4%';

UPDATE temp.ghg_report_2025_enriched SET pa_current_use = replace(pa_current_use,'3,4,1','')
WHERE pa_current_use LIKE '%3,4,1%';

UPDATE temp.ghg_report_2025_enriched SET pa_current_use = replace(pa_current_use,'1,4','')
WHERE pa_current_use LIKE '%1,4%';

UPDATE temp.ghg_report_2025_enriched SET pa_current_use = replace(pa_current_use,'1,','')
WHERE pa_current_use LIKE '%1,%';

UPDATE temp.ghg_report_2025_enriched SET pa_current_use = replace(pa_current_use,'4','Deer Management')
WHERE pa_current_use = '4';

-- D5) Conditions from final report + site_summary_2021 (same as original)
WITH conditions_all AS (
  SELECT grant_id,
         CASE
           WHEN peatland_condition ILIKE '%Other%' THEN regexp_replace(peatland_condition, '[^\\w\\s^,]', '', 'g') || ' - ' || notes
           ELSE regexp_replace(peatland_condition, '[^\\w\\s^,]', '', 'g')
         END AS peatland_condition
  FROM pa_final_report.site_outline
  UNION
  SELECT grant_reference AS grant_id, peat_condition_data AS peatland_condition
  FROM public.site_summary_2021
),
grant_id_unnest AS (
  SELECT centroid, unnest(string_to_array(grant_id, ',')) AS grant_id
  FROM temp.ghg_report_2025_enriched
),
join_condition_grant_id AS (
  SELECT centroid, a.grant_id, b.peatland_condition
  FROM grant_id_unnest a
  LEFT JOIN conditions_all b
    ON a.grant_id = b.grant_id
),
grant_id_nest AS (
  SELECT centroid,
         string_agg(distinct grant_id, ',') AS grant_id,
         string_agg(distinct peatland_condition, ',') AS peatland_condition
  FROM join_condition_grant_id
  GROUP BY centroid
  ORDER BY centroid
)
UPDATE temp.ghg_report_2025_enriched a
SET pa_condition_category = (
  SELECT peatland_condition FROM grant_id_nest b WHERE a.centroid = b.centroid
);

-- D6) Techniques from final reports (same structure as original)
WITH techniques_all AS (
  (SELECT foo.grant_id, foo.techniques
   FROM (
     WITH tecniques_grants AS (
       SELECT restoration_lines.grant_id,
              CASE
                WHEN restoration_lines.restoration_technique = 'Other (please specify in Notes)'::text THEN restoration_lines.notes
                ELSE restoration_lines.restoration_technique
              END AS technique
       FROM pa_final_report.restoration_lines
       UNION
       SELECT forest_to_bog.grant_id,
              CASE
                WHEN forest_to_bog.techniques = 'Other (please specify in Notes)'::text THEN forest_to_bog.notes
                ELSE forest_to_bog.techniques
              END AS technique
       FROM pa_final_report.forest_to_bog
       UNION
       SELECT forest_to_bog_details.grant_id,
              CASE
                WHEN forest_to_bog_details.technique = 'Other (please specify in Notes)'::text THEN forest_to_bog_details.notes
                ELSE forest_to_bog_details.technique
              END AS technique
       FROM pa_final_report.forest_to_bog_details
       UNION
       SELECT restoration_points.grant_id,
              CASE
                WHEN restoration_points.restoration_technique = 'Other (please specify in Notes)'::text THEN restoration_points.notes
                ELSE restoration_points.restoration_technique
              END AS technique
       FROM pa_final_report.restoration_points
       UNION
       SELECT restoration_areas.grant_id,
              CASE
                WHEN restoration_areas.restoration_technique = 'Other (please specify in Notes)'::text THEN restoration_areas.notes
                ELSE restoration_areas.restoration_technique
              END AS technique
       FROM pa_final_report.restoration_areas
       GROUP BY restoration_areas.grant_id,
                (CASE
                   WHEN restoration_areas.restoration_technique = 'Other (please specify in Notes)'::text THEN restoration_areas.notes
                   ELSE restoration_areas.restoration_technique
                 END)
       ORDER BY 1, 2
     )
     SELECT tecniques_grants.grant_id,
            string_agg(DISTINCT tecniques_grants.technique, ', '::text) AS techniques
     FROM tecniques_grants
     GROUP BY tecniques_grants.grant_id
   ) AS foo
  )
  UNION
  SELECT grant_reference AS grant_id, restoration_activities AS techniques
  FROM public.site_summary_2021
),
grant_id_unnest AS (
  SELECT centroid, unnest(string_to_array(grant_id, ',')) AS grant_id
  FROM temp.ghg_report_2025_enriched
),
join_techniques_grant_id AS (
  SELECT centroid, a.grant_id, b.techniques
  FROM grant_id_unnest a
  LEFT JOIN techniques_all b
    ON a.grant_id = b.grant_id
),
grant_id_nest AS (
  SELECT centroid,
         string_agg(distinct grant_id, ',') AS grant_id,
         string_agg(distinct techniques, ',') AS techniques
  FROM join_techniques_grant_id
  GROUP BY centroid
  ORDER BY centroid
)
UPDATE temp.ghg_report_2025_enriched a
SET techniques = (
  SELECT techniques FROM grant_id_nest b WHERE a.centroid = b.centroid
);

-- D7) Financial year end (min/max per centroid, set to max/to_year)
WITH grant_id_unnest AS (
  SELECT centroid, unnest(string_to_array(grant_id, ',')) AS grant_id
  FROM temp.ghg_report_2025_enriched
),
grant_id_fy AS (
  SELECT centroid, a.grant_id, b.financial_year_end
  FROM grant_id_unnest a
  LEFT JOIN pa_reporting.reported_ha b
    ON a.grant_id = b.grant_id
  ORDER BY centroid, grant_id
),
grant_id_fy_array AS (
  SELECT centroid,
         string_agg(distinct grant_id, ',') AS grant_id,
         min(financial_year_end) AS from_year,
         max(financial_year_end) AS to_year
  FROM grant_id_fy
  GROUP BY centroid
  ORDER BY centroid
)
UPDATE temp.ghg_report_2025_enriched a
SET financial_year_end = (SELECT to_year FROM grant_id_fy_array b WHERE a.centroid = b.centroid);

-- D8) Forestry flag + guess missing conditions (same rules)
UPDATE temp.ghg_report_2025_enriched
SET forestry = TRUE
WHERE pa_condition_category ILIKE '%forest%'
   OR pa_current_use ILIKE '%forest%'
   OR techniques SIMILAR TO '%(forest|stump|tree|smooth|mulching|regen|furrow|felling|scrub)%';

UPDATE temp.ghg_report_2025_enriched
SET pa_condition_category = 'Forested previously forested'
WHERE pa_condition_category IS NULL
  AND forestry = 'yes';

UPDATE temp.ghg_report_2025_enriched
SET pa_condition_category = 'Drained'
WHERE techniques = 'dams, ditch blocking';

UPDATE temp.ghg_report_2025_enriched
SET pa_condition_category = 'Not provided'
WHERE pa_condition_category = 'Yes' OR pa_condition_category = 'Y';

-- D9) Difference (baseline coverage area sum - project_area)
UPDATE temp.ghg_report_2025_enriched
SET difference = round((
  COALESCE("Broadleaved - Broadleaved - Forest",0)
+ COALESCE("Conifer - Conifer - Forest",0)
+ COALESCE("Cropland - Arable - Cropland",0)
+ COALESCE("Eroding - Eroding - Eroded",0)
+ COALESCE("Grassland - Extensive grassland - Extensive Grassland",0)
+ COALESCE("Grassland - Extensive grassland - Modified Bog (LCA Uplands Cor",0)
+ COALESCE("Grassland - Intensive grassland - Intensive Grassland",0)
+ COALESCE("Mapping offset - Mapping offset - Near Natural Bog",0)
+ COALESCE("Modified - Bracken-dominated - Modified Bog (LCA Uplands Correc",0)
+ COALESCE("Modified - Heather-dominated - Modified Bog",0)
+ COALESCE("Modified - Molinia-dominated - Modified Bog",0)
+ COALESCE("Other - No cover data - Near Natural Bog",0)
+ COALESCE("Peat extraction - Domestic or unknown - Domestic Extraction",0)
+ COALESCE("Peat extraction - Industrial - Industrial Extraction",0)
+ COALESCE("Scrub - Scrub - Forest",0)
+ COALESCE("Semi-natural - Near natural - Near Natural Bog",0)
+ COALESCE("Settlement - Settlement - Settlement",0)
+ COALESCE("Woodland - Mixed or unknown - Forest",0)
- COALESCE(project_area,0)
)::numeric, 2);

-- D10) Version as current date
UPDATE temp.ghg_report_2025_enriched
SET version = CURRENT_DATE;

DO $$
BEGIN
  RAISE NOTICE 'BLOCK D completed: Output ready in temp.ghg_report_2025_enriched.';
END $$;

-- =========================================================
-- BLOCK E — Create FINAL table (pa_ghg_reporting + YYYYMMDD suffix)
--           + cleaning + dedup + peat depth stats + warnings + grants
--           + drop ALL intermediate temp tables
--
-- Input : temp.ghg_report_2025_enriched   (from Block D)
-- Output: pa_ghg_reporting.ghg_report_2025_YYYYMMDD   (FINAL)
-- =========================================================

DO $$
DECLARE
  v_suffix text := to_char(CURRENT_DATE, 'YYYYMMDD');
  v_final  text := format('pa_ghg_reporting.ghg_report_2025_%s', to_char(CURRENT_DATE, 'YYYYMMDD'));
BEGIN
  RAISE NOTICE 'BLOCK E started: Create FINAL table % and run cleaning/enrichment steps.', v_final;

  -- E1) Create FINAL table (copy of enriched)
  EXECUTE format('DROP TABLE IF EXISTS %s', v_final);
  EXECUTE format('CREATE TABLE %s AS SELECT * FROM temp.ghg_report_2025_enriched', v_final);

  -- E2) Ensure version is current date (same as your script)
  EXECUTE format('UPDATE %s SET version = CURRENT_DATE', v_final);

  -- E3) CLEANING: delete small features (same threshold as your script: project_area < 33)
  EXECUTE format('DELETE FROM %s WHERE project_area < 33', v_final);

  -- E4) DELETE DUPLICATE GEOMS (same method as your script, but in temp)
  DROP TABLE IF EXISTS temp.ghg_report_no_dups;
  EXECUTE format('CREATE TABLE temp.ghg_report_no_dups AS SELECT row_number() OVER () AS id, * FROM %s', v_final);

  DELETE FROM temp.ghg_report_no_dups AS t
  WHERE EXISTS (
    SELECT 1
    FROM temp.ghg_report_no_dups AS _t
    WHERE _t.id < t.id
      AND ST_Equals(_t.geom, t.geom)
  );

  -- Replace FINAL with deduped version
  EXECUTE format('DROP TABLE IF EXISTS %s', v_final);
  EXECUTE format('ALTER TABLE temp.ghg_report_no_dups RENAME TO %I', format('ghg_report_2025_%s', v_suffix));
  -- (Now the renamed table lives in schema temp by default; move it to pa_ghg_reporting)
  EXECUTE format('ALTER TABLE temp.%I SET SCHEMA pa_ghg_reporting', format('ghg_report_2025_%s', v_suffix));

  -- Refresh v_final to point to the recreated final table
  v_final := format('pa_ghg_reporting.ghg_report_2025_%s', v_suffix);

  -- E5) ADDING peat depth stats (same logic as your script)
  DROP TABLE IF EXISTS temp.pdsconditions_deleteme;

  EXECUTE format($sql$
    CREATE TABLE temp.pdsconditions_deleteme AS
    (
      SELECT a.grant_id, a.geom, a.centroid,
        count(*) FILTER (WHERE condition = 'Actively eroding: Flat bare')*100/count(*) AS "%% Actively eroding: Flat bare",
        count(*) FILTER (WHERE condition = 'Actively eroding')*100/count(*) AS "%% Actively eroding",
        count(*) FILTER (WHERE condition = 'Actively eroding: Hagg/gully')*100/count(*) AS "%% Actively eroding: Hagg/gully",
        count(*) FILTER (WHERE condition = 'Modified')*100/count(*) AS "%% Modified",
        count(*) FILTER (WHERE condition = 'Drained: Artificial')*100/count(*) AS "%% Drained: Artificial",
        count(*) FILTER (WHERE condition = 'Forested / previously forested')*100/count(*) AS "%% Forested / previously forested",
        count(*) FILTER (WHERE condition = 'Drained')*100/count(*) AS "%% Drained",
        count(*) FILTER (WHERE condition = 'Drained: Hagg/gully')*100/count(*) AS "%% Drained: Hagg/gully",
        count(*) FILTER (WHERE condition = 'Near natural')*100/count(*) AS "%% Near natural",
        count(*) FILTER (WHERE condition = 'Not provided')*100/count(*) AS "%% Not provided",
        count(*) FILTER (WHERE condition = 'No peat')*100/count(*) AS "%% No peat",
        count(*) FILTER (WHERE condition = 'NA')*100/count(*) AS "%% NA"
      FROM %s a
      LEFT JOIN pa_peat_depth.combined_peat_depth b
        ON st_within(b.geom, a.geom)
      GROUP BY a.grant_id, a.geom, a.centroid
    )
  $sql$, v_final);

  -- Join peat depth stats back onto FINAL (same as your draft2 approach)
  DROP TABLE IF EXISTS temp.ghg_report_draft2;
  EXECUTE format('CREATE TABLE temp.ghg_report_draft2 AS SELECT * FROM %s', v_final);

  EXECUTE format('DROP TABLE IF EXISTS %s', v_final);

  EXECUTE format($sql$
    CREATE TABLE %s AS
    (
      SELECT a.*,
        b."%% Actively eroding: Flat bare",
        b."%% Actively eroding",
        b."%% Actively eroding: Hagg/gully",
        b."%% Modified",
        b."%% Drained: Artificial",
        b."%% Forested / previously forested",
        b."%% Drained",
        b."%% Drained: Hagg/gully",
        b."%% Near natural",
        b."%% Not provided",
        b."%% No peat",
        b."%% NA"
      FROM temp.ghg_report_draft2 a
      LEFT JOIN temp.pdsconditions_deleteme b
        ON a.geom = b.geom
    )
  $sql$, v_final);

  -- E6) ADD warnings (same logic as your script: warn sites derived from centroid buffers/site boundaries set)
  EXECUTE format('ALTER TABLE %s ADD COLUMN warning varchar', v_final);

  EXECUTE format($sql$
    UPDATE %s a
    SET warning = 'It could contain boundaries derived from point centroids or site boundaries instead of the restoration footprint'
    FROM (
      SELECT foo.grant_reference AS grant_id
      FROM (
        SELECT row_number() OVER (ORDER BY site_summary_2021.grant_reference) AS id,
               site_summary_2021.grant_reference,
               site_summary_2021.project_name,
               site_summary_2021.site_name,
               site_summary_2021.financial_year_of_application,
               site_summary_2021.ha_reported_total,
               site_summary_2021.ha_reported_1213,
               site_summary_2021.ha_reported_1314,
               site_summary_2021.ha_reported_1415,
               site_summary_2021.ha_reported_1516,
               site_summary_2021.ha_reported_1617,
               site_summary_2021.ha_reported_1718,
               site_summary_2021.ha_reported_1819,
               site_summary_2021.ha_reported_1920,
               site_summary_2021.ha_reported_2021,
               st_buffer(site_summary_2021.geom,
                 sqrt(site_summary_2021.ha_reported_total * 10000::double precision / 3.14::double precision)
               ) AS geometry,
               sqrt(site_summary_2021.ha_reported_total * 10000::double precision / 3.14::double precision) AS radius_meters
        FROM site_summary_2021
        WHERE site_summary_2021.project_type ~~* '%%restoration%%'::text
          AND NOT (site_summary_2021.grant_reference IN (SELECT DISTINCT grant_id FROM pa_final_report.restoration_footprint_30))
          AND site_summary_2021.project_status ~~* '%%completed%%'::text
          AND site_summary_2021.ha_reported_total > 0::double precision
      ) AS foo
      GROUP BY grant_reference
    ) b
    WHERE a.grant_id = b.grant_id
  $sql$, v_final);

  -- E7) ADD PERMISSIONS (same grants as your script)
  EXECUTE format('GRANT ALL ON TABLE %s TO edit', v_final);
  EXECUTE format('GRANT SELECT ON TABLE %s TO pa_readaccess', v_final);
  EXECUTE format('GRANT SELECT ON TABLE %s TO pao_russell', v_final);
  EXECUTE format('GRANT ALL ON TABLE %s TO s_long', v_final);
  EXECUTE format('GRANT ALL ON TABLE %s TO t_finucane', v_final);

  -- Keep your baseline grants unchanged (same as script)
  EXECUTE 'GRANT ALL ON TABLE external_data.ghg_20240131 TO edit';
  EXECUTE 'GRANT SELECT ON TABLE external_data.ghg_20240131 TO pa_readaccess';
  EXECUTE 'GRANT SELECT ON TABLE external_data.ghg_20240131 TO pao_russell';
  EXECUTE 'GRANT ALL ON TABLE external_data.ghg_20240131 TO s_long';
  EXECUTE 'GRANT ALL ON TABLE external_data.ghg_20240131 TO t_finucane';

  RAISE NOTICE 'BLOCK E completed: FINAL table created: %', v_final;
END $$;

-- =========================================================
-- BLOCK E.8 — Drop intermediate tables (ONLY temp schema)
-- (Final table in pa_ghg_reporting is kept.)
-- =========================================================

DO $$
BEGIN
  RAISE NOTICE 'BLOCK E cleanup started: Dropping intermediate temp tables.';

  DROP TABLE IF EXISTS temp.ghg_report_temp;
  DROP TABLE IF EXISTS temp.ghg_report_temp_poly;
  DROP TABLE IF EXISTS temp.ghg_report_temp_poly_by_year;
  DROP TABLE IF EXISTS temp.ghg_report_resolve_overlap;
  DROP TABLE IF EXISTS temp.ghg_report_resolve_overlap_round2;
  DROP TABLE IF EXISTS temp.ghg_report_2025_work;
  DROP TABLE IF EXISTS temp.ghg_20240131_clip_dissolve;
  DROP TABLE IF EXISTS temp.ghg_report_2025_draft;
  DROP TABLE IF EXISTS temp.ghg_report_2025_enriched;
  DROP TABLE IF EXISTS temp.pdsconditions_deleteme;
  DROP TABLE IF EXISTS temp.ghg_report_draft2;

  RAISE NOTICE 'BLOCK E cleanup completed: All intermediate temp tables dropped. Final table kept in pa_ghg_reporting.';
END $$;

-- =========================================================
-- BLOCK F — Change tracking vs previous snapshot + QC
-- FINAL table: pa_ghg_reporting.ghg_report_2025_YYYYMMDD
-- Previous : pa_ghg_reporting.ghg_report_20240904
-- =========================================================

DO $$
DECLARE
  v_final text := format(
    'pa_ghg_reporting.ghg_report_2025_%s',
    to_char(CURRENT_DATE, 'YYYYMMDD')
  );
  v_ha numeric;
BEGIN
  RAISE NOTICE 'BLOCK F started on %', v_final;

  -- -----------------------------------------------------
  -- F1) Add change tracking columns (idempotent)
  -- -----------------------------------------------------
  EXECUTE format(
    'ALTER TABLE %s ADD COLUMN IF NOT EXISTS new_site boolean',
    v_final
  );

  EXECUTE format(
    'ALTER TABLE %s ADD COLUMN IF NOT EXISTS changed boolean',
    v_final
  );

  -- -----------------------------------------------------
  -- F2) new_site flag
  -- -----------------------------------------------------
  EXECUTE format(
    'UPDATE %s SET new_site = FALSE
     WHERE grant_id IN (
       SELECT grant_id
       FROM pa_ghg_reporting.ghg_report_20240904
     )',
    v_final
  );

  EXECUTE format(
    'UPDATE %s SET new_site = TRUE
     WHERE grant_id NOT IN (
       SELECT grant_id
       FROM pa_ghg_reporting.ghg_report_20240904
     )',
    v_final
  );

  -- -----------------------------------------------------
  -- F3) changed flag (centroid comparison)
  -- -----------------------------------------------------
  EXECUTE format(
    'UPDATE %s SET changed = NULL',
    v_final
  );

  EXECUTE format(
    'UPDATE %s SET changed = TRUE
     WHERE new_site = FALSE
       AND centroid NOT IN (
         SELECT centroid
         FROM pa_ghg_reporting.ghg_report_20240904
       )',
    v_final
  );

  EXECUTE format(
    'UPDATE %s SET changed = FALSE
     WHERE changed IS DISTINCT FROM TRUE',
    v_final
  );

  -- -----------------------------------------------------
  -- QC1) Total area (ha)
  -- -----------------------------------------------------
  EXECUTE format(
    'SELECT round(sum(project_area)/10000::numeric,2)
     FROM %s',
    v_final
  )
  INTO v_ha;

  RAISE NOTICE 'QC — Total area (ha): %', v_ha;

  -- -----------------------------------------------------
  -- QC2) Summary counts
  -- -----------------------------------------------------
  EXECUTE format(
    'SELECT count(*) FROM %s',
    v_final
  )
  INTO v_ha;

  RAISE NOTICE 'QC — Total rows: %', v_ha;

  RAISE NOTICE 'BLOCK F completed successfully on %', v_final;
END $$;

SELECT round(sum(project_area)/10000::numeric,2) FROM pa_ghg_reporting.ghg_report_2025;