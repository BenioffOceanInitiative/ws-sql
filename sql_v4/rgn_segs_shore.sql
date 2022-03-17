--# add column touches_coast
ALTER TABLE `{tbl_rgn_segs}` 
  ADD COLUMN IF NOT EXISTS touches_coast BOOL;

UPDATE `{tbl_rgn_segs}` SET
  touches_coast = CASE WHEN	
    ST_INTERSECTS(
      linestring,
      (SELECT ST_UNION_AGG(geom) FROM ((
        SELECT * FROM `{tbl_shore}`))))
	  THEN TRUE
	  ELSE FALSE;
  -- # When linestring INTERSECTS a DISSOLVED coastline feature, touches_coast IS TRUE, ELSE touches_coast IS FALSE.
  -- # FLAGS linestrings that intersect coastline, mostly around ports.

