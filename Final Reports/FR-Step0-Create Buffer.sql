--http://cliffpatterson.ca/blog/2017/10/06/merging-and-dissolving-several-polygon-layers-using-postgis/

DROP TABLE IF EXISTS pa_final_report.restoration_footprint_50;
CREATE TABLE pa_final_report.restoration_footprint_50

AS (

with restoration_lines_buffered AS (

-- restoration lines

SELECT grant_id, site_id, st_multi(st_union(st_buffer(st_MakeValid(geom),50))) AS geom
FROM pa_final_report.restoration_lines
WHERE restoration_technique not ilike '%Track%'
group by grant_id, site_id),

restoration_point_buffered as (

-- restoration points

SELECT grant_id, site_id, st_multi(st_union(st_buffer(st_MakeValid(geom),50))) AS geom
FROM pa_final_report.restoration_points
group by grant_id, site_id),

restoration_areas_buffered as (
-- restoration areas
SELECT grant_id, site_id,
st_multi(st_union(st_buffer(st_MakeValid(geom),
CASE WHEN restoration_technique = 'scrub removal' then 0
WHEN restoration_technique = 'scrub removal' then 0
ELSE 50 END ))) AS geom
FROM pa_final_report.restoration_areas
group by grant_id, site_id),

restoration_bare_peat as (

-- restoration bare peat

SELECT grant_id, site_id,
st_multi(st_union(st_buffer(st_MakeValid(geom),50))) AS geom
FROM pa_final_report.bare_peat_stabilisation
group by grant_id, site_id),

-- restoration f2b
restoration_f2b as (
SELECT grant_id, site_id,
st_multi(st_union(st_buffer(st_MakeValid(geom),0))) AS geom
FROM pa_final_report.forest_to_bog
group by grant_id, site_id),

-- merge all datasets

union_dataset AS (
SELECT grant_id, site_id, geom FROM restoration_lines_buffered
UNION
SELECT grant_id, site_id, geom FROM restoration_point_buffered
UNION
SELECT grant_id, site_id, geom FROM restoration_areas_buffered
UNION 
SELECT grant_id, site_id, geom FROM restoration_f2b
),

dissolved_dataset AS (
SELECT grant_id, site_id, ST_Union(st_MakeValid(geom)) as geom
 FROM union_dataset group by grant_id, site_id)


SELECT grant_id, site_id, (ST_DUMP(st_MakeValid(geom))).geom::geometry(Polygon,27700) AS geom 
    FROM dissolved_dataset);

ALTER TABLE pa_final_report.restoration_footprint_50
ADD COLUMN id SERIAL PRIMARY KEY;
--here

ALTER TABLE pa_final_report.restoration_footprint_50 RENAME TO restoration_footprint_50_single;

CREATE TABLE pa_final_report.restoration_footprint_50 AS (
    SELECT grant_id, site_id, ST_Multi(ST_Union(geom)) AS geom
    FROM pa_final_report.restoration_footprint_50_single
    GROUP BY grant_id, site_id);

SELECT ST_GeometryType(geom) FROM pa_final_report.restoration_footprint_50;

INSERT INTO pa_final_report.restoration_footprint_50 (grant_id, site_id, geom)
SELECT grant_id, site_id, geom FROM pa_final_report.restoration_footprint 
WHERE concat(grant_id, '--', site_id) not in (SELECT distinct concat(grant_id, '--', site_id) FROM pa_final_report.restoration_footprint_50);

DROP table if exists pa_final_report.restoration_footprint_50_single;

-----------30---------------------------

--http://cliffpatterson.ca/blog/2017/10/06/merging-and-dissolving-several-polygon-layers-using-postgis/

DROP TABLE IF EXISTS pa_final_report.restoration_footprint_30;
CREATE TABLE pa_final_report.restoration_footprint_30

AS (

with restoration_lines_buffered AS (

-- restoration lines

SELECT grant_id, site_id, st_multi(st_union(st_buffer(st_MakeValid(geom),30))) AS geom
FROM pa_final_report.restoration_lines
WHERE restoration_technique not ilike '%Track%'
group by grant_id, site_id),

restoration_point_buffered as (

-- restoration points

SELECT grant_id, site_id, st_multi(st_union(st_buffer(st_MakeValid(geom),30))) AS geom
FROM pa_final_report.restoration_points
group by grant_id, site_id),

restoration_areas_buffered as (
-- restoration areas
SELECT grant_id, site_id,
st_multi(st_union(st_buffer(st_MakeValid(geom),
CASE WHEN restoration_technique = 'scrub removal' then 0
WHEN restoration_technique = 'scrub removal' then 0
ELSE 30 END ))) AS geom
FROM pa_final_report.restoration_areas
group by grant_id, site_id),

restoration_bare_peat as (

-- restoration bare peat

SELECT grant_id, site_id,
st_multi(st_union(st_buffer(st_MakeValid(geom),30))) AS geom
FROM pa_final_report.bare_peat_stabilisation
group by grant_id, site_id),

-- restoration f2b
restoration_f2b as (
SELECT grant_id, site_id,
st_multi(st_union(st_buffer(st_MakeValid(geom),0))) AS geom
FROM pa_final_report.forest_to_bog
group by grant_id, site_id),

-- merge all datasets

union_dataset AS (
SELECT grant_id, site_id, geom FROM restoration_lines_buffered
UNION
SELECT grant_id, site_id, geom FROM restoration_point_buffered
UNION
SELECT grant_id, site_id, geom FROM restoration_areas_buffered
UNION 
SELECT grant_id, site_id, geom FROM restoration_f2b
),

dissolved_dataset AS (
SELECT grant_id, site_id, ST_Union(st_MakeValid(geom)) as geom
 FROM union_dataset group by grant_id, site_id)

SELECT grant_id, site_id, (ST_DUMP(st_MakeValid(geom))).geom::geometry(Polygon,27700) AS geom 
    FROM dissolved_dataset);

--here

ALTER TABLE pa_final_report.restoration_footprint_30 RENAME TO restoration_footprint_30_single;

CREATE TABLE pa_final_report.restoration_footprint_30 AS (
    SELECT grant_id, site_id, ST_Multi(ST_Union(geom)) AS geom
    FROM pa_final_report.restoration_footprint_30_single
    GROUP BY grant_id, site_id);

SELECT ST_GeometryType(geom) FROM pa_final_report.restoration_footprint_30;

INSERT INTO pa_final_report.restoration_footprint_30 (grant_id, site_id, geom)
SELECT grant_id, site_id, geom FROM pa_final_report.restoration_footprint 
WHERE concat(grant_id, '--', site_id) not in (SELECT distinct concat(grant_id, '--', site_id) FROM pa_final_report.restoration_footprint_30);

DROP table if exists pa_final_report.restoration_footprint_30_single;

--ALTER TABLE pa_final_report.restoration_footprint_30
--ADD COLUMN id SERIAL PRIMARY KEY;


--ELECT grant_id, site_id, ST_GeometryN(geom, 1) FROM pa_final_report.restoration_footprint WHERE grant_id = '501613';