--# References:
--# * [Enhancing Geospatial in BigQuery with CARTO Spatial Extension | CARTO Blog](https://carto.com/blog/enhancing-geospatial-in-bigquery-with-carto-spatial-extension/)
--# * [Analytics Toolbox for BigQuery | CARTO Documentation](https://docs.carto.com/analytics-toolbox-bq/sql-reference/h3/#st_ash3_polyfill)

DECLARE h3res_val INT64 DEFAULT 4;
-- DECLARE h3res_val INT64 DEFAULT 5;
-- DECLARE h3res_val INT64 DEFAULT 6;
-- DECLARE h3res_val INT64 DEFAULT 7; --# slow: 9 min 46 sec

-- DROP TABLE IF EXISTS `benioff-ocean-initiative.whalesafe_v4.rgns_h3`;
CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe_v4.rgns_h3` (
    rgn STRING, 
    h3res INT64, 
    h3id STRING, 
    geog GEOGRAPHY)
CLUSTER BY rgn, h3res, h3id
OPTIONS (
  description              = "regional hexagons clustered by (rgn, h3res, h3id)");

DELETE  FROM `benioff-ocean-initiative.whalesafe_v4.rgns_h3` WHERE h3res = h3res_val;

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
SELECT rgn, h3res_val AS h3res, h3id, h3.geog
FROM h3;
