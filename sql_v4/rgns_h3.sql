--# References:
--# * [Enhancing Geospatial in BigQuery with CARTO Spatial Extension | CARTO Blog](https://carto.com/blog/enhancing-geospatial-in-bigquery-with-carto-spatial-extension/)
--# * [Analytics Toolbox for BigQuery | CARTO Documentation](https://docs.carto.com/analytics-toolbox-bq/sql-reference/h3/#st_ash3_polyfill)

--# ran for all values: 4, 5, 6, 7
DECLARE h3res_val INT64 DEFAULT 4;

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

--# delete h3res_val from table rgns_h3
DELETE  FROM `benioff-ocean-initiative.whalesafe_v4.rgns_h3` WHERE h3res = h3res_val;

--# insert new h3res hexagons using Carto's H3 library
INSERT `benioff-ocean-initiative.whalesafe_v4.rgns_h3` (rgn, h3res, h3id, geog)
WITH 
  input as (
    SELECT rgn, bqcarto.h3.ST_ASH3_POLYFILL(geog, h3res_val) AS h3id 
    FROM `benioff-ocean-initiative.whalesafe_v4`.rgns
  ),
  h3 AS (
    SELECT rgn, h3id, bqcarto.h3.ST_BOUNDARY(h3id) as geog
    FROM input, UNNEST(h3id) as h3id
  )

--# TEST: https://bigquerygeoviz.appspot.com
--# Query: 
-- SELECT rgn, h3id, geog
-- FROM `benioff-ocean-initiative.whalesafe_v4.rgns_h3`
-- WHERE h3res = 4;
