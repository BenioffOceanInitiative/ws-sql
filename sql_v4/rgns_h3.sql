--# References:
--# * [Enhancing Geospatial in BigQuery with CARTO Spatial Extension | CARTO Blog](https://carto.com/blog/enhancing-geospatial-in-bigquery-with-carto-spatial-extension/)
--# * [Analytics Toolbox for BigQuery | CARTO Documentation](https://docs.carto.com/analytics-toolbox-bq/sql-reference/h3/#st_ash3_polyfill)

--# create table rgns_h3
-- DROP TABLE IF EXISTS `benioff-ocean-initiative.whalesafe_v4.rgns_h3`;
CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe_v4.rgns_h3` (
  rgn STRING, 
  h3res INT64, 
  h3id STRING, 
  geog GEOGRAPHY)
CLUSTER BY rgn, h3res, h3id
OPTIONS (
  description = "regional hexagons clustered by (rgn, h3res, h3id)");

--# ran for all values: 4, 5, 6, 7
DECLARE h3res_val INT64 DEFAULT 4;
DECLARE h3res_buf INT64 DEFAULT 22*4*1000;
-- DECLARE h3res_val INT64 DEFAULT 5;
-- DECLARE h3res_buf INT64 DEFAULT 8*4*1000;
-- DECLARE h3res_val INT64 DEFAULT 6;
-- DECLARE h3res_buf INT64 DEFAULT 3*4*1000;
-- DECLARE h3res_val INT64 DEFAULT 7;
-- DECLARE h3res_buf INT64 DEFAULT 1*4*1000;
-- Add buffer to ensure hexagons extend to entirety of region:
-- * [Ensuring H3 Polyfill Returns all Hexagons For a Polygon / Rusty Conover / Observable](https://observablehq.com/@rustyconover/ensuring-h3-polyfill-returns-all-hexagons-for-a-polygon)
-- * [Table of Cell Areas for H3 Resolutions | H3](https://h3geo.org/docs/core-library/restable/)

--# delete h3res_val from table rgns_h3
DELETE  FROM `benioff-ocean-initiative.whalesafe_v4.rgns_h3` WHERE h3res = h3res_val;

--# insert new h3res hexagons using Carto's H3 library
INSERT `benioff-ocean-initiative.whalesafe_v4.rgns_h3` (rgn, h3res, h3id, geog)
WITH 
  b as (
    SELECT rgn, ST_BUFFER(ST_SIMPLIFY(geog, h3res_buf/4), h3res_buf) AS geog
    FROM `benioff-ocean-initiative.whalesafe_v4.rgns`
  ),
  i as (
    SELECT rgn, bqcarto.h3.ST_ASH3_POLYFILL(geog, h3res_val) AS h3id 
    FROM b
  ),
  h as (
    SELECT rgn, h3res_val AS h3res, h3id, bqcarto.h3.ST_BOUNDARY(h3id) as geog
    FROM i, UNNEST(h3id) as h3id),
  r as (
    SELECT ST_UNION_AGG(geog) AS geog
    FROM `benioff-ocean-initiative.whalesafe_v4.rgns`
  )
  SELECT h.*
  FROM h, r
  WHERE ST_INTERSECTS(h.geog, r.geog);

--# TEST: https://bigquerygeoviz.appspot.com
--# Query: 
SELECT rgn, h3id, geog
FROM `benioff-ocean-initiative.whalesafe_v4.rgns_h3`
WHERE h3res = 4 AND rgn = 'USA-East';


