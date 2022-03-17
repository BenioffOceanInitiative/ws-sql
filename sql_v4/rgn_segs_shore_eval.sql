--# add column touches_coast
ALTER TABLE `benioff-ocean-initiative.whalesafe_v4.rgn_segs` 
  ADD COLUMN IF NOT EXISTS touches_shore BOOL;

UPDATE `benioff-ocean-initiative.whalesafe_v4.rgn_segs` segs
SET touches_shore = TRUE
FROM `benioff-ocean-initiative.whalesafe_v4.shore` shore
WHERE 
 DATE(timestamp) > '1900-01-01' AND
 touches_shore IS NULL AND 
 ST_Intersects(shore.geog, segs.linestring);

UPDATE `benioff-ocean-initiative.whalesafe_v4.rgn_segs` segs
SET touches_shore = FALSE
WHERE 
 DATE(timestamp) > '1900-01-01' AND
 touches_shore IS NULL;
-- # FLAGS linestrings that intersect coastline, mostly around ports.

-- CHECK at https://bigquerygeoviz.appspot.com: SELECT * FROM benioff-ocean-initiative.whalesafe_v4.rgn_segs WHERE DATE(timestamp) > DATE('1900-01-01') AND touches_shore = FALSE;
