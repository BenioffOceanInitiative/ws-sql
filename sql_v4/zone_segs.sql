--# original: ais_vsr_segments.sql

--# create table

--# clear zone_segs
-- DELETE FROM `whalesafe_v4.zone_segs` WHERE DATE(timestamp) > DATE('2017-01-01');

--# get date from which to start inserting segments based on max previous
DECLARE date_beg DEFAULT(
	SELECT
		COALESCE( MAX(DATE(timestamp)), DATE('{date_beg}') )
		FROM `whalesafe_v4.zone_segs`
	WHERE
		DATE(timestamp) >= DATE('{date_beg}') AND
		DATE(timestamp) <= DATE('{date_end}') AND
		zone = '{zone}'
	LIMIT 1);
-- SELECT date_beg;

--# delete segments spanning max date to most recent
DELETE FROM `whalesafe_v4.zone_segs`
WHERE 
	rgn = '{rgn}' AND
	zone = '{zone}' AND
	DATE(timestamp) >= date_beg AND
	DATE(timestamp) <= DATE('{date_end}');

--# insert segments
INSERT INTO `whalesafe_v4.zone_segs` (
	rgn, 
	zone, 
	mmsi,
	num,
	timestamp,
	timestamp_beg,
	timestamp_end,
	final_speed_knots, 
	speed_knots,
	implied_speed_knots,
	calculated_knots,
	distance_km,
	distance_nm,
	segment_time_minutes,
	lon,
	lat,
	source,
	seg_id,
	good_seg,
	overlapping_and_short,
	-- gt, -- TODO: gross tonnage
	point,
	-- linestring, -- of non-intersected segment
	touches_shore,
	speedbin_num,
	speedbin_str,
	speedbin_implied_num,
	speedbin_implied_str,
	speedbin_calculated_num,
	speedbin_calculated_str,
	speedbin_final_num,
	speedbin_final_str,
	length_m, -- TODO: similarly recalculate true begin/end of intersected segment
	geog
)
WITH
	z AS (
		SELECT rgn, zone, geog 
		FROM `whalesafe_v4.zones`
		WHERE 
			rgn  = '{rgn}' AND
			zone = '{zone}' ),
	s AS (
		SELECT *
		FROM `{tbl_rgn_segs}`
		WHERE 
			rgn = '{rgn}' AND
			DATE(timestamp) >= date_beg AND
			DATE(timestamp) <= DATE('{date_end}') ),
	x AS (
		SELECT 
			z.zone,
			s.*, 
			ST_UNION(ST_DUMP(ST_INTERSECTION(z.geog, s.linestring), 1)) AS geog
		FROM z INNER JOIN s 
			ON ST_Intersects(z.geog, s.linestring) )
SELECT 
	rgn, 
	zone, 
	mmsi,
	num,
	timestamp,
	timestamp_beg,
	timestamp_end,
	final_speed_knots, 
	speed_knots,
	implied_speed_knots,
	calculated_knots,
	distance_km,
	distance_nm,
	segment_time_minutes,
	lon,
	lat,
	source,
	seg_id,
	good_seg,
	overlapping_and_short,
	-- gt, -- TODO: gross tonnage
	point,
	-- linestring, -- of non-intersected segment
	touches_shore,
	speedbin_num,
	speedbin_str,
	speedbin_implied_num,
	speedbin_implied_str,
	speedbin_calculated_num,
	speedbin_calculated_str,
	speedbin_final_num,
	speedbin_final_str,
	ST_LENGTH(geog) AS length_m, -- TODO: similarly recalculate true begin/end of intersected segment
	geog
FROM x;

--# TEST: https://bigquerygeoviz.appspot.com
--# Query: 
-- SELECT 
--   final_speed_knots, geog
-- FROM 
--   `whalesafe_v4.zone_segs`
-- WHERE
-- 	zone = '{zone}' AND
-- 	DATE(timestamp) >= DATE('{date_beg}') AND
-- 	DATE(timestamp) <= DATE('{date_end}');
--# Style: 
--# - Data-driven; linear; pct_length_gt10knots; 
--# - Domain: 0, 0.5, 1
--# - Range: #4CAF50 (green), #FFC107 (yellow), #F44336 (red)