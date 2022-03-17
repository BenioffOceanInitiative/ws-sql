-- # Updating `whalesafe_v4.ais_segments`
-- # Benioff Ocean Initiative: 2022-02-10

-- # Step 0:
-- # IF STARTING FROM SCRATCH, USE FIRST DECLARE STATEMENT & COMMENT THE SECOND DECLARE STATEMENT BELOW:

-- DECLARE
-- new_seg_ts DEFAULT(
-- SELECT
-- SAFE_CAST ('1990-01-01 00:00:00' AS TIMESTAMP));

-- # Step 1: If UPDATING `whalesafe_v3.ais_segments`, DECLARE THE NEWEST timestamp AS new_seg_ts (USE BELOW)
-- DECLARE
-- 	new_seg_ts DEFAULT(
-- 		SELECT
-- 			MAX(timestamp_end)
-- 			FROM `whalesafe_v4.ais_segments`
-- 		WHERE
-- 			DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
-- 		LIMIT 1);

-- # Step 2: Create temporary table to hold new segments data
CREATE TEMPORARY TABLE `temp_ais_segments` (
	timestamp TIMESTAMP,
	date DATE,
	mmsi INT64,
	num INT64,
	timestamp_beg TIMESTAMP,
	timestamp_end TIMESTAMP,
	speed_knots NUMERIC,
	implied_speed_knots NUMERIC,
	calculated_knots NUMERIC,
	distance_km NUMERIC,
	distance_nm NUMERIC,
	segment_time_minutes FLOAT64,
	lon FLOAT64,
	lat FLOAT64,
	source STRING,
	region STRING,
	seg_id STRING,
	good_seg BOOL,
	overlapping_and_short BOOL,
	gt NUMERIC,
	point GEOGRAPHY,
	linestring GEOGRAPHY,
	final_speed_knots NUMERIC,
	-- touches_coast BOOL,
	speed_bin_num INT64,
	implied_speed_bin_num INT64,
	calculated_speed_bin_num INT64,
	final_speed_bin_num INT64
	);

-- # Step 3: Query `whalesafe_v3.ais_data` and insert the constructed vessel segments into the temporary table
INSERT INTO `temp_ais_segments`
SELECT * ,
	CASE 
		WHEN speed_knots = 0 THEN 0
		WHEN speed_knots > 0 AND speed_knots <= 10 THEN 1
		WHEN speed_knots > 10	AND speed_knots <= 12 THEN 2
		WHEN speed_knots > 12 AND speed_knots <= 15 THEN 3
		WHEN speed_knots > 15 AND speed_knots <= 50 THEN 4 
		ELSE 5
	END AS speed_bin_num,
	-- Assign speed bin number for 'speed_knots'
	CASE
		WHEN implied_speed_knots = 0 THEN	0
		WHEN implied_speed_knots > 0 AND implied_speed_knots <= 10 THEN 1
		WHEN implied_speed_knots > 10	AND implied_speed_knots <= 12 THEN 2
		WHEN implied_speed_knots > 12 AND implied_speed_knots <= 15 THEN 3
		WHEN implied_speed_knots > 15 AND implied_speed_knots <= 50 THEN 4
		ELSE 5
	END AS implied_speed_bin_num,
	-- Assign speed bin number for 'implied_speed'
	CASE
		WHEN calculated_knots = 0 THEN 0
		WHEN calculated_knots > 0 AND calculated_knots <= 10 THEN	1
		WHEN calculated_knots > 10 AND calculated_knots <= 12 THEN 2
		WHEN calculated_knots > 12 AND calculated_knots <= 15 THEN 3
		WHEN calculated_knots > 15 AND calculated_knots <= 50 THEN 4
		ELSE 5
	END AS calculated_speed_bin_num,
	-- Assign speed bin number for 'calculated_knots'
	CASE
		WHEN final_speed_knots = 0 THEN 0
		WHEN final_speed_knots > 0 AND final_speed_knots <= 10 THEN 1
		WHEN final_speed_knots > 10 AND final_speed_knots <= 12 THEN 2
		WHEN final_speed_knots > 12	AND final_speed_knots <= 15 THEN 3
		WHEN final_speed_knots > 15	AND final_speed_knots <= 50 THEN 4
		ELSE 5
	END AS final_speed_bin_num
	-- # Assign speed bin number for 'final_speed_knots'.
	-- # This is the field used for aggregation in the segments_agg script`
FROM (
	SELECT
		*,
		ROUND((CASE 
			WHEN speed_knots BETWEEN 0.001 AND 50 THEN speed_knots
			WHEN calculated_knots BETWEEN 0.001 AND 50 THEN calculated_knots
			WHEN implied_speed_knots BETWEEN 0.001 AND 50 THEN implied_speed_knots
			ELSE NULL
			END), 3) AS final_speed_knots,
		-- CASE WHEN
		-- 	-- # intersects CA coast
		-- 	ST_INTERSECTS(
		-- 		linestring, 
		-- 		(SELECT ST_UNION_AGG(geom) FROM ((
		-- 			SELECT * FROM `whalesafe_v3.cali_us_coast_medium`))))
		-- 	THEN TRUE
		-- 	ELSE FALSE
		-- END AS touches_coast
		-- # When linestring INTERSECTS a DISSOLVED US California coastline feature, touches_coast IS TRUE, ELSE touches_coast IS FALSE.
		-- # FLAGS linestrings that intersect coastline, mostly around ports.
		-- # TODO: GET BETTER/MORE DETAILED POLYGON FOR PORTS AND COASTLINES.
	FROM (
		SELECT
			timestamp,
			DATE(timestamp) AS date,
			mmsi,
			num,
			SAFE_CAST (t1 AS TIMESTAMP) AS timestamp_beg,
			SAFE_CAST (t2 AS TIMESTAMP) AS timestamp_end,
			ROUND(speed_knots, 4) AS speed_knots,
			ROUND(implied_speed_knots, 4) AS implied_speed_knots,
			-- # If the time elapsed between points is greater than 0 milliseconds, then calculate the speed in knots, otherwise return 0 knots
			ROUND(SAFE_CAST (
			CASE WHEN (TIMESTAMP_DIFF ((t2),(t1),MILLISECOND)) > 0
			THEN
			ROUND(((SAFE_CAST (st_distance(geom2, geom) / 1000 AS NUMERIC) / (TIMESTAMP_DIFF ((t2), (t1), MILLISECOND) / 3600000)) * 0.539957), 4)
			ELSE
			NULL
			END AS numeric),4) AS calculated_knots,
			-- # Get distance in kilometers
			ROUND(SAFE_CAST (st_distance(geom2, geom) / 1000 AS NUMERIC), 5) AS distance_km,
			-- # Get distance in nautical miles
			ROUND(SAFE_CAST (st_distance(geom2, geom) * 0.000539957 AS NUMERIC), 5) AS distance_nm,
			-- # Get time elapsed between points in minutes
			ROUND((TIMESTAMP_DIFF ((t2), (t1), SECOND) / 60), 2) AS segment_time_minutes,
			lon,
			lat,
			# AIS message source
			source,
			region,
			seg_id,
			good_seg,
			overlapping_and_short,
			gt,
			-- # Point Geography
			geom AS point,
			-- geom AS point_beg,
			-- geom2 AS point_end,
			-- # Linestring Geography
			ST_MAKELINE (geom, geom2) AS linestring
		FROM (
			SELECT 
				* EXCEPT (geom2, geom, t1, t2),
				row_number() OVER w1 AS num, -- # assign row number for each segment using WINDOW FUNCTION BELOW
				ST_GeogPoint (lon, lat) AS geom, -- # Create point 1 for ST_MAKELINE function above.
				LEAD(ST_GeogPoint (lon, lat)) OVER w1 AS geom2, -- # Create point 2 for ST_MAKELINE function above.
				LEAD( (timestamp), 0) OVER w1 AS t1, -- # t1 for point 1
				LEAD( (timestamp), 1) OVER w1 AS t2,
				ROUND((ST_DISTANCE(LEAD(ST_GeogPoint (lon, lat)) OVER w1, ST_GeogPoint (lon, lat)) / 1000), 2) AS distance_km,
				ROUND(SAFE_CAST((ST_DISTANCE(LEAD(ST_GeogPoint (lon, lat)) OVER w1, ST_GeogPoint (lon, lat)) * 0.000539957) AS NUMERIC), 2) AS distance_nm,
				ROUND((TIMESTAMP_DIFF((LEAD( (timestamp), 1) OVER w1),(LEAD( (timestamp), 0) OVER w1), SECOND) / 60),2) AS min,
				ROUND(
				SAFE_CAST(
				SAFE_DIVIDE(
				(ST_DISTANCE(LEAD(ST_GeogPoint (lon, lat)) OVER w1, ST_GeogPoint (lon, lat)) * 0.000539957) ,
				(TIMESTAMP_DIFF((LEAD( (timestamp), 1) OVER w1),(LEAD( (timestamp), 0) OVER w1), SECOND) / 3600))
				AS NUMERIC), 2)
				AS calculated_knots
			FROM (
				SELECT *
				FROM (
					SELECT *,
						CASE
							WHEN calc_knots BETWEEN 0.001 AND 50 AND lag_calc_knots BETWEEN 0.001 AND 50 THEN TRUE
							ELSE FALSE
						END AS good_point,
						CASE 
							WHEN
								ABS(implied_speed_knots - speed_knots) > 8 AND
								ABS(calc_knots - speed_knots) > 8 AND
								ABS(lag_calc_knots - speed_knots) > 8
							THEN TRUE
							ELSE FALSE
						END AS speed_diff_8,
					FROM (
						SELECT
							(ais.mmsi),
							timestamp,
							DATE(timestamp) AS date,
							SAFE_CAST (LEAD((speed_knots), 1) OVER w AS NUMERIC) AS speed_knots,
							SAFE_CAST (LEAD((implied_speed_knots), 1) OVER w AS NUMERIC) AS implied_speed_knots,
							lon,
							lat,
							source,
							seg_id,
							good_seg,
							overlapping_and_short,
							region,
							ihs_data.gt,
							(ST_DISTANCE(LEAD(ST_GeogPoint (lon, lat)) OVER w, ST_GeogPoint (lon, lat)) * 0.000539957) AS dist_nm,
							SAFE_CAST(
							SAFE_DIVIDE(
							(ST_DISTANCE(LEAD(ST_GeogPoint (lon, lat)) OVER w, ST_GeogPoint (lon, lat)) * 0.000539957) ,
							(TIMESTAMP_DIFF((LEAD( (timestamp), 1) OVER w),(LEAD( (timestamp), 0) OVER w), SECOND) / 3600))
							AS NUMERIC)
							AS calc_knots,
							SAFE_CAST(
							SAFE_DIVIDE(
							(ST_DISTANCE(LAG(ST_GeogPoint (lon, lat)) OVER w, ST_GeogPoint (lon, lat)) * 0.000539957) ,
							(TIMESTAMP_DIFF((LEAD( (timestamp), 0) OVER w),(LAG( (timestamp), 1) OVER w), SECOND) / 3600))
							AS NUMERIC)
							AS lag_calc_knots,
							ST_GeogPoint (lon, lat) AS geom, -- # Create point 1 for ST_MAKELINE function above.
							LEAD(ST_GeogPoint (lon, lat)) OVER w AS geom2, -- # Create point 2 for ST_MAKELINE function above.
							LEAD( (timestamp), 0) OVER w AS t1, -- # t1 for point 1
							LEAD( (timestamp), 1) OVER w AS t2, -- # t2 for point 2
							-- ST_MAKELINE(LEAD(ST_GeogPoint (lon, lat)) OVER w, (ST_GeogPoint (lon, lat)) ) AS line
						FROM
							`whalesafe_v3.ais_data` AS ais
							LEFT JOIN
							`whalesafe_v3.ihs_data_all` AS ihs_data
							ON 
								ais.mmsi = ihs_data.mmsi AND 
								DATE(ais.timestamp) >= ihs_data.start_date AND
								DATE(ais.timestamp) <= ihs_data.end_date
						WHERE
							(timestamp) > new_seg_ts AND
							ihs_data.gt >= 300 AND -- # gross tonnage must be greater than or equal to 300
							--AND good_seg IS TRUE
							speed_knots > 0.001 AND 
							implied_speed_knots > 0.001 AND 
							overlapping_and_short IS FALSE
						WINDOW w AS (PARTITION BY ais.mmsi ORDER BY timestamp)
					)
					WHERE 
						dist_nm > 0 AND 
						dist_nm <= 120 AND 
						geom2 IS NOT NULL
				)
				WHERE
					good_point IS TRUE AND 
					speed_diff_8 IS FALSE
			)
			WINDOW w1 AS (PARTITION BY mmsi, date ORDER BY timestamp)
		)
		WHERE 
			geom2 IS NOT NULL
	)
);

--# Step 4: CREATE PARTITIONED AND CLUSTERED ` whalesafe_v3.ais_segments` table IF NOT EXISTS
CREATE TABLE IF NOT EXISTS `whalesafe_v3.ais_segments` (
	timestamp TIMESTAMP,
	date DATE,
	mmsi INT64,
	num INT64,
	timestamp_beg TIMESTAMP,
	timestamp_end TIMESTAMP,
	speed_knots NUMERIC,
	implied_speed_knots NUMERIC,
	calculated_knots NUMERIC,
	distance_km NUMERIC,
	distance_nm NUMERIC,
	segment_time_minutes FLOAT64,
	lon FLOAT64,
	lat FLOAT64,
	source STRING,
	region STRING,
	seg_id STRING,
	good_seg BOOL,
	overlapping_and_short BOOL,
	gt NUMERIC,
	point GEOGRAPHY,
	linestring GEOGRAPHY,
	final_speed_knots NUMERIC,
	touches_coast BOOL,
	speed_bin_num INT64,
	implied_speed_bin_num INT64,
	calculated_speed_bin_num INT64,
	final_speed_bin_num INT64)
PARTITION BY DATE(timestamp) 
CLUSTER BY
	mmsi, region, point, linestring 
OPTIONS (
	description = "partitioned by day, clustered by (mmsi, point, linestring)",   
	require_partition_filter = TRUE);

--# Step 5: Insert everything from temporary table into ` whalesafe_v3.ais_segments`
INSERT INTO `whalesafe_v3.ais_segments`
SELECT *
FROM `temp_ais_segments`;

--# Step 6: Make whalesafe_v3 timestamp log table if not already existing.
CREATE TABLE IF NOT EXISTS `whalesafe_v3.whalesafe_timestamp_log` (
	newest_timestamp TIMESTAMP,
	date_accessed TIMESTAMP,
	table_name STRING,
	query_exec STRING
);

--# Step 7: Insert 'new_seg_ts', the new timestamp in `ais_segments` from BEFORE querying ais_data
INSERT INTO `whalesafe_v3.whalesafe_timestamp_log`
SELECT
	new_seg_ts AS newest_timestamp,
	CURRENT_TIMESTAMP() AS date_accessed,
	'ais_segments' AS table_name,
	'query_start' AS query_exec;

--# Step 8: Insert 'new_seg_ts', the new timestamp in `ais_segments` from AFTER querying ais_data
INSERT INTO `whalesafe_v3.whalesafe_timestamp_log`
SELECT 
	( SELECT MAX(timestamp_end)
		FROM `whalesafe_v3.ais_segments`
		WHERE
		DATE(timestamp) > DATE_SUB(DATE(new_seg_ts), INTERVAL 3 MONTH)
		LIMIT 1
	) AS newest_timestamp,
	CURRENT_TIMESTAMP() AS date_accessed,
	'ais_segments' AS table_name,
	'query_end' AS query_exec;
