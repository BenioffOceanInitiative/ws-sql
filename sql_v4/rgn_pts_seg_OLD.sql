--# OLD: [ais_data.sql](https://github.com/BenioffOceanInitiative/ws-sql/blob/2bb89c2c96cf199b9e93d63fd54742f020c2c5a0/sql_v4/ais_data.sql)

--# TODO: deduplicate based on msgid
-- SELECT COUNT(DISTINCT msgid) FROM whalesafe_v4.rgn_pts;
--# unique msgid: 902,344,392; nrow: 902,344,395

CREATE OR REPLACE TABLE whalesafe_v4.rgn_pts AS
SELECT DISTINCT * FROM whalesafe_v4.rgn_pts group by 1

ALTER TABLE whalesafe_v4.rgn_pts
ADD COLUMN IF NOT EXISTS mmsi INT64,
ADD COLUMN IF NOT EXISTS good_seg BOOL,
ADD COLUMN IF NOT EXISTS overlapping_and_short BOOL;
--# TODO: set description (see rgn_pts; get GFW descriptions)

UPDATE `benioff-ocean-initiative.whalesafe_v4.rgn_pts` SET
  mmsi = SAFE_CAST(ssvid AS INT64)
  WHERE DATE(timestamp) > DATE('1900-01-01');  --# CAST ssvid to NUMERIC and rename AS mmsi
--# these are already proper data type, so presumably don't need to run:
--  speed_knots         = SAFE_CAST (ais.speed_knots AS NUMERIC),     --# CAST speed_knots to NUMERIC
--  implied_speed_knots = SAFE_CAST (implied_speed_knots AS NUMERIC); --# CAST implied_speed_knots to NUMERIC

MERGE `benioff-ocean-initiative.whalesafe_v4.rgn_pts` AS p
USING `world-fishing-827.gfw_research.pipe_v20201001_segs` AS s
ON p.seg_id = s.seg_id
WHEN MATCHED AND (p.good_seg IS NULL OR p.overlapping_and_short IS NULL) THEN
  UPDATE SET
    good_seg              = s.good_seg,
    overlapping_and_short = s.overlapping_and_short;

--# TODO: update logs.
--#  see OLD Step 6,7 in [ais_data.sql](https://github.com/BenioffOceanInitiative/ws-sql/blob/2bb89c2c96cf199b9e93d63fd54742f020c2c5a0/sql_v4/ais_data.sql)


-- FIXES BEFORE FOLDING INTO rgn_pts.sql...

UPDATE `benioff-ocean-initiative.whalesafe_v4.rgn_pts` SET
  mmsi = SAFE_CAST (ssvid AS INT64)
  WHERE DATE(timestamp) > DATE('1900-01-01');

  SELECT * FROM `benioff-ocean-initiative.whalesafe_v4.rgn_pts`;                        -- 1,039,712,524 rows
  SELECT * FROM `benioff-ocean-initiative.whalesafe_v4.rgn_pts` WHERE mmsi IS NOT NULL; --    41,665,201 rows

UPDATE `benioff-ocean-initiative.whalesafe_v4.rgn_pts` SET
  mmsi = SAFE_CAST(ssvid AS INT64)
  WHERE mmsi IS NULL AND timestamp IS NOT NULL;

SELECT COUNT(*) AS cnt FROM `benioff-ocean-initiative.whalesafe_v4.rgn_pts` WHERE mmsi IS NOT NULL; -- 1,039,712,524 rows

MERGE `benioff-ocean-initiative.whalesafe_v4.rgn_pts` AS p
USING `world-fishing-827.gfw_research.pipe_v20201001_segs` AS s
ON p.seg_id = s.seg_id
WHEN MATCHED AND (p.good_seg IS NULL OR p.overlapping_and_short IS NULL) THEN
  UPDATE SET
    good_seg              = s.good_seg,
    overlapping_and_short = s.overlapping_and_short;

ALTER TABLE `benioff-ocean-initiative.whalesafe_v4.rgn_pts` DROP COLUMN ssvid;