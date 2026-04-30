-- =========================================================
-- BLOCK A
-- Polygon-only + spatially indexed
-- Source: pa_application.restoration_footprint_30
-- Output: pa_application.restoration_footprint_30_poly
-- =========================================================


DROP TABLE IF EXISTS pa_application.restoration_footprint_30_poly;

-- 1) Create without id
CREATE TABLE pa_application.restoration_footprint_30_poly AS
SELECT
  grant_id,
  site_id,
  ST_SetSRID(
    ST_MakeValid((ST_Dump(geom)).geom),
    27700
  )::geometry(Polygon, 27700) AS geom
FROM pa_application.restoration_footprint_30
WHERE geom IS NOT NULL;

-- Optional: remove empties that can appear after makevalid/dump
DELETE FROM pa_application.restoration_footprint_30_poly
WHERE geom IS NULL OR ST_IsEmpty(geom);

-- 2) Add a guaranteed-unique id
ALTER TABLE pa_application.restoration_footprint_30_poly
  ADD COLUMN id bigint GENERATED ALWAYS AS IDENTITY;

-- 3) Make it the primary key
ALTER TABLE pa_application.restoration_footprint_30_poly
  ADD CONSTRAINT restoration_footprint_30_poly_pkey PRIMARY KEY (id);

-- Indexes
CREATE INDEX restoration_footprint_30_poly_geom_gix
  ON pa_application.restoration_footprint_30_poly
  USING gist (geom);

CREATE INDEX restoration_footprint_30_poly_grant_site_idx
  ON pa_application.restoration_footprint_30_poly (grant_id, site_id);

ANALYZE pa_application.restoration_footprint_30_poly;

SELECT ST_GeometryType(geom) AS geom_type, COUNT(*) AS n
FROM pa_application.restoration_footprint_30_poly
GROUP BY 1;



-- =========================================================
-- BLOCK B — Build working footprint table (applications run)
-- Uses: pa_application.restoration_footprint_30_poly   <-- Option 1
-- Creates: temp.ghg_report_2025_work_applications
-- =========================================================

DO $$
BEGIN
  RAISE NOTICE 'BLOCK B started: Build working footprint table from pa_application.restoration_footprint_30_poly.';
END $$;

DROP TABLE IF EXISTS temp.ghg_report_2025_work_applications;

CREATE TABLE temp.ghg_report_2025_work_applications AS
SELECT
  grant_id::text AS grant_id,
  geom
FROM pa_application.restoration_footprint_30_poly
WHERE geom IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ghg_report_2025_work_applications_geom
  ON temp.ghg_report_2025_work_applications USING gist (geom);

ANALYZE temp.ghg_report_2025_work_applications;

DO $$
BEGIN
  RAISE NOTICE 'BLOCK B completed: temp.ghg_report_2025_work_applications ready.';
END $$;



-- =========================================================
-- BLOCK C — Clip baseline + dissolve + transpose (applications)
-- =========================================================

DO $$
BEGIN
  RAISE NOTICE 'BLOCK C started: Clip GHG baseline to application footprints.';
END $$;

-- Ensure spatial indexes exist
CREATE INDEX IF NOT EXISTS idx_ghg_baseline_geom
  ON external_data.ghg_20240131 USING gist (geom);

CREATE INDEX IF NOT EXISTS idx_work_applications_geom
  ON temp.ghg_report_2025_work_applications USING gist (geom);

ANALYZE external_data.ghg_20240131;
ANALYZE temp.ghg_report_2025_work_applications;

-- C1) Clip + dissolve
DROP TABLE IF EXISTS temp.ghg_20240131_clip_dissolve_applications;

CREATE TABLE temp.ghg_20240131_clip_dissolve_applications AS
WITH clipped AS (
  SELECT
    p.grant_id,
    g.lc_level2,
    g.lc_level3,
    g.condition,
    ST_Intersection(g.geom, p.geom) AS geom,
    ROUND(ST_Area(p.geom)::numeric, 2) AS project_area,
    p.geom AS geom2
  FROM temp.ghg_report_2025_work_applications p
  JOIN external_data.ghg_20240131 g
    ON ST_Intersects(g.geom, p.geom)
  WHERE NOT ST_IsEmpty(ST_Intersection(g.geom, p.geom))
)
SELECT
  row_number() OVER () AS id,
  grant_id,
  lc_level2,
  lc_level3,
  condition,
  project_area,
  (ST_Dump(ST_Union(geom))).geom AS geom,
  geom2
FROM clipped
GROUP BY grant_id, lc_level2, lc_level3, condition, project_area, geom2;

CREATE INDEX IF NOT EXISTS idx_clip_dissolve_applications_geom
  ON temp.ghg_20240131_clip_dissolve_applications USING gist (geom);

ANALYZE temp.ghg_20240131_clip_dissolve_applications;

-- C2) Transpose rows → columns (wide)
DROP TABLE IF EXISTS temp.ghg_report_2025_draft_applications;

CREATE TABLE temp.ghg_report_2025_draft_applications AS
WITH conditions AS (
  SELECT
    grant_id,
    lc_level2 || ' - ' || lc_level3 || ' - ' || condition AS emissions_category,
    project_area,
    ROUND(SUM(ST_Area(geom))::numeric, 2) AS emissions_area,
    geom2
  FROM temp.ghg_20240131_clip_dissolve_applications
  GROUP BY grant_id, emissions_category, project_area, geom2
)
SELECT
  grant_id,
  project_area,

  COALESCE(MAX(CASE WHEN emissions_category = 'Broadleaved - Broadleaved - Forest' THEN emissions_area END), 0) AS "Broadleaved - Broadleaved - Forest",
  COALESCE(MAX(CASE WHEN emissions_category = 'Conifer - Conifer - Forest' THEN emissions_area END), 0) AS "Conifer - Conifer - Forest",
  COALESCE(MAX(CASE WHEN emissions_category = 'Cropland - Arable - Cropland' THEN emissions_area END), 0) AS "Cropland - Arable - Cropland",
  COALESCE(MAX(CASE WHEN emissions_category = 'Eroding - Eroding - Eroded' THEN emissions_area END), 0) AS "Eroding - Eroding - Eroded",
  COALESCE(MAX(CASE WHEN emissions_category = 'Grassland - Extensive grassland - Extensive Grassland' THEN emissions_area END), 0) AS "Grassland - Extensive grassland - Extensive Grassland",
  COALESCE(MAX(CASE WHEN emissions_category = 'Grassland - Extensive grassland - Modified Bog (LCA Uplands Correction)' THEN emissions_area END), 0) AS "Grassland - Extensive grassland - Modified Bog (LCA Uplands Correction)",
  COALESCE(MAX(CASE WHEN emissions_category = 'Grassland - Intensive grassland - Intensive Grassland' THEN emissions_area END), 0) AS "Grassland - Intensive grassland - Intensive Grassland",
  COALESCE(MAX(CASE WHEN emissions_category = 'Mapping offset - Mapping offset - Near Natural Bog' THEN emissions_area END), 0) AS "Mapping offset - Mapping offset - Near Natural Bog",
  COALESCE(MAX(CASE WHEN emissions_category = 'Modified - Bracken-dominated - Modified Bog (LCA Uplands Correction)' THEN emissions_area END), 0) AS "Modified - Bracken-dominated - Modified Bog (LCA Uplands Correction)",
  COALESCE(MAX(CASE WHEN emissions_category = 'Modified - Heather-dominated - Modified Bog' THEN emissions_area END), 0) AS "Modified - Heather-dominated - Modified Bog",
  COALESCE(MAX(CASE WHEN emissions_category = 'Modified - Molinia-dominated - Modified Bog' THEN emissions_area END), 0) AS "Modified - Molinia-dominated - Modified Bog",
  COALESCE(MAX(CASE WHEN emissions_category = 'Other - No cover data - Near Natural Bog' THEN emissions_area END), 0) AS "Other - No cover data - Near Natural Bog",
  COALESCE(MAX(CASE WHEN emissions_category = 'Peat extraction - Domestic or unknown - Domestic Extraction' THEN emissions_area END), 0) AS "Peat extraction - Domestic or unknown - Domestic Extraction",
  COALESCE(MAX(CASE WHEN emissions_category = 'Peat extraction - Industrial - Industrial Extraction' THEN emissions_area END), 0) AS "Peat extraction - Industrial - Industrial Extraction",
  COALESCE(MAX(CASE WHEN emissions_category = 'Scrub - Scrub - Forest' THEN emissions_area END), 0) AS "Scrub - Scrub - Forest",
  COALESCE(MAX(CASE WHEN emissions_category = 'Semi-natural - Near natural - Near Natural Bog' THEN emissions_area END), 0) AS "Semi-natural - Near natural - Near Natural Bog",
  COALESCE(MAX(CASE WHEN emissions_category = 'Settlement - Settlement - Settlement' THEN emissions_area END), 0) AS "Settlement - Settlement - Settlement",
  COALESCE(MAX(CASE WHEN emissions_category = 'Woodland - Mixed or unknown - Forest' THEN emissions_area END), 0) AS "Woodland - Mixed or unknown - Forest",

  geom2 AS geom
FROM conditions
GROUP BY grant_id, project_area, geom
ORDER BY grant_id;

CREATE INDEX IF NOT EXISTS idx_ghg_report_2025_draft_applications_geom
  ON temp.ghg_report_2025_draft_applications USING gist (geom);

ANALYZE temp.ghg_report_2025_draft_applications;

DO $$
BEGIN
  RAISE NOTICE 'BLOCK C completed: temp.ghg_report_2025_draft_applications ready.';
END $$;


-- =========================================================
-- BLOCK D — Enrich attributes (applications)
-- =========================================================

DO $$
BEGIN
  RAISE NOTICE 'BLOCK D started: Enrich draft table with metadata.';
END $$;

DROP TABLE IF EXISTS temp.ghg_report_2025_enriched_applications;

CREATE TABLE temp.ghg_report_2025_enriched_applications AS
SELECT * FROM temp.ghg_report_2025_draft_applications;

ALTER TABLE temp.ghg_report_2025_enriched_applications
  ADD COLUMN IF NOT EXISTS difference numeric,
  ADD COLUMN IF NOT EXISTS project_name varchar,
  ADD COLUMN IF NOT EXISTS financial_year_end int,
  ADD COLUMN IF NOT EXISTS centroid varchar,
  ADD COLUMN IF NOT EXISTS forestry boolean DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS version date;

-- Centroid (grid ref)
UPDATE temp.ghg_report_2025_enriched_applications
SET centroid = get_grid_ref_from_geom(ST_PointOnSurface(geom));

-- Project name + FY end (authoritative source)
UPDATE temp.ghg_report_2025_enriched_applications a
SET
  project_name = b.project_name,
  financial_year_end = b.financial_year_end
FROM pa_metadata.grant_reference b
WHERE a.grant_id = b.grant_id::text;

-- Forestry flag (same heuristics as legacy)
UPDATE temp.ghg_report_2025_enriched_applications
SET forestry = TRUE
WHERE project_name ILIKE '%forest%';

-- Difference (baseline area − project area)
UPDATE temp.ghg_report_2025_enriched_applications
SET difference = round((
  COALESCE("Broadleaved - Broadleaved - Forest",0)
+ COALESCE("Conifer - Conifer - Forest",0)
+ COALESCE("Cropland - Arable - Cropland",0)
+ COALESCE("Eroding - Eroding - Eroded",0)
+ COALESCE("Grassland - Extensive grassland - Extensive Grassland",0)
+ COALESCE("Grassland - Extensive grassland - Modified Bog (LCA Uplands Correction)",0)
+ COALESCE("Grassland - Intensive grassland - Intensive Grassland",0)
+ COALESCE("Mapping offset - Mapping offset - Near Natural Bog",0)
+ COALESCE("Modified - Bracken-dominated - Modified Bog (LCA Uplands Correction)",0)
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

UPDATE temp.ghg_report_2025_enriched_applications
SET version = CURRENT_DATE;

DO $$
BEGIN
  RAISE NOTICE 'BLOCK D completed: temp.ghg_report_2025_enriched_applications ready.';
END $$;


-- =========================================================
-- BLOCK E — Create FINAL table (pa_ghg_reporting + YYYYMMDD suffix)
--           + cleaning + dedup + peat depth stats + grants
--
-- Input : temp.ghg_report_2025_enriched_applications
-- Output: pa_ghg_reporting.ghg_report_2025_applications_YYYYMMDD   (FINAL)
-- =========================================================

DO $$
DECLARE
  v_suffix text := to_char(CURRENT_DATE, 'YYYYMMDD');
  v_final  text := format('pa_ghg_reporting.ghg_report_2025_applications_%s', v_suffix);
BEGIN
  RAISE NOTICE 'BLOCK E started: Create FINAL table % and run cleaning/enrichment steps.', v_final;

  -- E1) Create FINAL table (copy of enriched)
  EXECUTE format('DROP TABLE IF EXISTS %s', v_final);
  EXECUTE format('CREATE TABLE %s AS SELECT * FROM temp.ghg_report_2025_enriched_applications', v_final);

  -- E2) Ensure version is current date
  EXECUTE format('UPDATE %s SET version = CURRENT_DATE', v_final);

  -- E3) CLEANING: delete small features (threshold: project_area < 33)
  EXECUTE format('DELETE FROM %s WHERE project_area < 33', v_final);

  -- E4) DELETE DUPLICATE GEOMS (in temp, then replace FINAL)
  DROP TABLE IF EXISTS temp.ghg_report_no_dups_applications;
  EXECUTE format(
    'CREATE TABLE temp.ghg_report_no_dups_applications AS
     SELECT row_number() OVER () AS id, * FROM %s',
    v_final
  );

  DELETE FROM temp.ghg_report_no_dups_applications AS t
  WHERE EXISTS (
    SELECT 1
    FROM temp.ghg_report_no_dups_applications AS _t
    WHERE _t.id < t.id
      AND ST_Equals(_t.geom, t.geom)
  );

  -- Replace FINAL with deduped version
  EXECUTE format('DROP TABLE IF EXISTS %s', v_final);

  EXECUTE format(
    'ALTER TABLE temp.ghg_report_no_dups_applications RENAME TO %I',
    format('ghg_report_2025_applications_%s', v_suffix)
  );

  -- Move it into pa_ghg_reporting
  EXECUTE format(
    'ALTER TABLE temp.%I SET SCHEMA pa_ghg_reporting',
    format('ghg_report_2025_applications_%s', v_suffix)
  );

  -- Refresh v_final after move
  v_final := format('pa_ghg_reporting.ghg_report_2025_applications_%s', v_suffix);

  -- Optional but recommended: index final geom for later use
  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS %I ON %s USING gist (geom)',
    format('idx_ghg_report_2025_applications_%s_geom', v_suffix),
    v_final
  );

  -- E5) ADD peat depth stats
  DROP TABLE IF EXISTS temp.pdsconditions_deleteme_applications;

  EXECUTE format($sql$
    CREATE TABLE temp.pdsconditions_deleteme_applications AS
    (
      SELECT a.grant_id, a.geom, a.centroid,
        count(*) FILTER (WHERE condition = 'Actively eroding: Flat bare')*100.0/nullif(count(*),0) AS "%% Actively eroding: Flat bare",
        count(*) FILTER (WHERE condition = 'Actively eroding')*100.0/nullif(count(*),0) AS "%% Actively eroding",
        count(*) FILTER (WHERE condition = 'Actively eroding: Hagg/gully')*100.0/nullif(count(*),0) AS "%% Actively eroding: Hagg/gully",
        count(*) FILTER (WHERE condition = 'Modified')*100.0/nullif(count(*),0) AS "%% Modified",
        count(*) FILTER (WHERE condition = 'Drained: Artificial')*100.0/nullif(count(*),0) AS "%% Drained: Artificial",
        count(*) FILTER (WHERE condition = 'Forested / previously forested')*100.0/nullif(count(*),0) AS "%% Forested / previously forested",
        count(*) FILTER (WHERE condition = 'Drained')*100.0/nullif(count(*),0) AS "%% Drained",
        count(*) FILTER (WHERE condition = 'Drained: Hagg/gully')*100.0/nullif(count(*),0) AS "%% Drained: Hagg/gully",
        count(*) FILTER (WHERE condition = 'Near natural')*100.0/nullif(count(*),0) AS "%% Near natural",
        count(*) FILTER (WHERE condition = 'Not provided')*100.0/nullif(count(*),0) AS "%% Not provided",
        count(*) FILTER (WHERE condition = 'No peat')*100.0/nullif(count(*),0) AS "%% No peat",
        count(*) FILTER (WHERE condition = 'NA')*100.0/nullif(count(*),0) AS "%% NA"
      FROM %s a
      LEFT JOIN pa_peat_depth.combined_peat_depth b
        ON ST_Within(b.geom, a.geom)
      GROUP BY a.grant_id, a.geom, a.centroid
    )
  $sql$, v_final);

  -- Join peat depth stats back onto FINAL (draft2 approach)
  DROP TABLE IF EXISTS temp.ghg_report_draft2_applications;
  EXECUTE format('CREATE TABLE temp.ghg_report_draft2_applications AS SELECT * FROM %s', v_final);

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
      FROM temp.ghg_report_draft2_applications a
      LEFT JOIN temp.pdsconditions_deleteme_applications b
        ON a.geom = b.geom
    )
  $sql$, v_final);

  -- Recreate spatial index after rebuild
  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS %I ON %s USING gist (geom)',
    format('idx_ghg_report_2025_applications_%s_geom', v_suffix),
    v_final
  );

  -- E6) Warnings removed (site_summary_2021 no longer used)

  -- E7) ADD PERMISSIONS
  EXECUTE format('GRANT ALL ON TABLE %s TO edit', v_final);
  EXECUTE format('GRANT SELECT ON TABLE %s TO pa_readaccess', v_final);
  EXECUTE format('GRANT SELECT ON TABLE %s TO pao_russell', v_final);
  EXECUTE format('GRANT ALL ON TABLE %s TO s_long', v_final);
  EXECUTE format('GRANT ALL ON TABLE %s TO t_finucane', v_final);

  -- Baseline grants unchanged
  EXECUTE 'GRANT ALL ON TABLE external_data.ghg_20240131 TO edit';
  EXECUTE 'GRANT SELECT ON TABLE external_data.ghg_20240131 TO pa_readaccess';
  EXECUTE 'GRANT SELECT ON TABLE external_data.ghg_20240131 TO pao_russell';
  EXECUTE 'GRANT ALL ON TABLE external_data.ghg_20240131 TO s_long';
  EXECUTE 'GRANT ALL ON TABLE external_data.ghg_20240131 TO t_finucane';

  RAISE NOTICE 'BLOCK E completed: FINAL table created: %', v_final;
END $$;


-- =========================================================
-- BLOCK E.8 — Drop intermediate tables (ONLY temp schema)
-- =========================================================

DO $$
BEGIN
  RAISE NOTICE 'BLOCK E cleanup started: Dropping intermediate temp tables (applications).';

  DROP TABLE IF EXISTS temp.ghg_report_2025_work_applications;
  DROP TABLE IF EXISTS temp.ghg_20240131_clip_dissolve_applications;
  DROP TABLE IF EXISTS temp.ghg_report_2025_draft_applications;
  DROP TABLE IF EXISTS temp.ghg_report_2025_enriched_applications;

  DROP TABLE IF EXISTS temp.pdsconditions_deleteme_applications;
  DROP TABLE IF EXISTS temp.ghg_report_draft2_applications;

  -- If created earlier:
  DROP TABLE IF EXISTS temp.ghg_report_no_dups_applications;

  RAISE NOTICE 'BLOCK E cleanup completed: All intermediate temp tables dropped. Final table kept in pa_ghg_reporting.';
END $$;
