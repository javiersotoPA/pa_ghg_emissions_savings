-- =========================================================
-- FULL BLOCK: APPLICATION FOOTPRINT (30m) using FY end > 2025
-- - NO imported_date
-- - FY filter comes from pa_metadata.grant_reference
-- - Joins by grant_id
-- - Includes: points, lines, areas, bare peat, forest to bog
-- - FINAL TABLE NAME NOT CHANGED: pa_application.restoration_footprint_30
-- =========================================================

DROP TABLE IF EXISTS pa_application.restoration_footprint_30;

CREATE TABLE pa_application.restoration_footprint_30 AS
WITH
eligible_grants AS (
  SELECT gr.grant_id::text AS grant_id
  FROM pa_metadata.grant_reference gr
  WHERE gr.financial_year_end > 2025
),

-- -----------------------------
-- LINES (buffer 30m)
-- -----------------------------
restoration_lines_buffered AS (
  SELECT
    rl.grant_id::text AS grant_id,
    rl.site_id,
    ST_Multi(
      ST_Union(
        ST_Buffer(ST_MakeValid(rl.geom), 30)
      )
    ) AS geom
  FROM pa_application.restoration_lines rl
  JOIN eligible_grants eg
    ON eg.grant_id = rl.grant_id::text
  WHERE rl.geom IS NOT NULL
    AND rl.restoration_technique NOT ILIKE '%Track%'
  GROUP BY rl.grant_id, rl.site_id
),

-- -----------------------------
-- POINTS (buffer 30m)
-- -----------------------------
restoration_points_buffered AS (
  SELECT
    rp.grant_id::text AS grant_id,
    rp.site_id,
    ST_Multi(
      ST_Union(
        ST_Buffer(ST_MakeValid(rp.geom), 30)
      )
    ) AS geom
  FROM pa_application.restoration_points rp
  JOIN eligible_grants eg
    ON eg.grant_id = rp.grant_id::text
  WHERE rp.geom IS NOT NULL
  GROUP BY rp.grant_id, rp.site_id
),

-- -----------------------------
-- AREAS (buffer 30m except scrub removal = 0)
-- -----------------------------
restoration_areas_buffered AS (
  SELECT
    ra.grant_id::text AS grant_id,
    ra.site_id,
    ST_Multi(
      ST_Union(
        ST_Buffer(
          ST_MakeValid(ra.geom),
          CASE
            WHEN ra.restoration_technique = 'scrub removal' THEN 0
            ELSE 30
          END
        )
      )
    ) AS geom
  FROM pa_application.restoration_areas ra
  JOIN eligible_grants eg
    ON eg.grant_id = ra.grant_id::text
  WHERE ra.geom IS NOT NULL
  GROUP BY ra.grant_id, ra.site_id
),

-- -----------------------------
-- BARE PEAT (buffer 30m)
-- -----------------------------
bare_peat_buffered AS (
  SELECT
    bp.grant_id::text AS grant_id,
    bp.site_id,
    ST_Multi(
      ST_Union(
        ST_Buffer(ST_MakeValid(bp.geom), 30)
      )
    ) AS geom
  FROM pa_application.bare_peat_stabilisation bp
  JOIN eligible_grants eg
    ON eg.grant_id = bp.grant_id::text
  WHERE bp.geom IS NOT NULL
  GROUP BY bp.grant_id, bp.site_id
),

-- -----------------------------
-- FOREST TO BOG (0 buffer / just make valid)
-- -----------------------------
forest_to_bog_union AS (
  SELECT
    f2b.grant_id::text AS grant_id,
    f2b.site_id,
    ST_Multi(
      ST_Union(
        ST_Buffer(ST_MakeValid(f2b.geom), 0)
      )
    ) AS geom
  FROM pa_application.forest_to_bog f2b
  JOIN eligible_grants eg
    ON eg.grant_id = f2b.grant_id::text
  WHERE f2b.geom IS NOT NULL
  GROUP BY f2b.grant_id, f2b.site_id
),

-- -----------------------------
-- UNION ALL SOURCES
-- -----------------------------
union_dataset AS (
  SELECT grant_id, site_id, geom FROM restoration_lines_buffered
  UNION ALL
  SELECT grant_id, site_id, geom FROM restoration_points_buffered
  UNION ALL
  SELECT grant_id, site_id, geom FROM restoration_areas_buffered
  UNION ALL
  SELECT grant_id, site_id, geom FROM bare_peat_buffered
  UNION ALL
  SELECT grant_id, site_id, geom FROM forest_to_bog_union
),

-- -----------------------------
-- DISSOLVE PER GRANT + SITE
-- -----------------------------
dissolved_dataset AS (
  SELECT
    grant_id,
    site_id,
    ST_Union(ST_MakeValid(geom)) AS geom
  FROM union_dataset
  GROUP BY grant_id, site_id
)

-- -----------------------------
-- OUTPUT: singlepart polygons (one row per polygon part)
-- -----------------------------
SELECT
  grant_id,
  site_id,
  (ST_Dump(ST_MakeValid(geom))).geom::geometry(Polygon,27700) AS geom
FROM dissolved_dataset;

-- Helpful indexes (optional but recommended)
CREATE INDEX IF NOT EXISTS restoration_footprint_30_gix
  ON pa_application.restoration_footprint_30
  USING gist (geom);

CREATE INDEX IF NOT EXISTS restoration_footprint_30_grant_site_idx
  ON pa_application.restoration_footprint_30 (grant_id, site_id);

ANALYZE pa_application.restoration_footprint_30;
