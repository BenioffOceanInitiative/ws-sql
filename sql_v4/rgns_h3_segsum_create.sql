--# create table if needed
-- DROP TABLE IF EXISTS `{tbl_rgns_h3_segsum}`;
CREATE TABLE IF NOT EXISTS `{tbl_rgns_h3_segsum}` (
  rgn STRING, 
  h3res INT64, 
  h3id STRING, 
  length_m_gt10knots FLOAT64, 
  length_m_all FLOAT64,
  pct_length_gt10knots FLOAT64,
  period STRING,
  date_min DATE,
  date_max DATE,
  hexbin_num INT64,
  hexbin_str STRING )
CLUSTER BY rgn, h3res, h3id
OPTIONS (
  description = "regional hexagon segments clustered by (rgn, h3res, h3id)");