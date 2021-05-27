-- # -- # Updating `whalesafe_v3.operator_stats_annual` -- # --
-- # Benioff Ocean Initiative: 2020-07-30
-- # -- # Needs `ship_stats_daily` to run.

-- # -- Step 0: Drop existing table.
DROP TABLE IF EXISTS `whalesafe_v3.operator_stats_annual`;

-- # -- Step 1: Create new operator_stats_annual table.
CREATE TABLE IF NOT EXISTS `whalesafe_v3.operator_stats_annual` CLUSTER BY
		operator,
		operator_ship_categories,
		year,
		coop_score_annual AS
    SELECT
			operator,
			operator_code,
      operator_ship_categories,
      vsr_region,
			year,
			ROUND(coop_score, 2) AS coop_score_annual ,
			CONCAT(
			ROUND(coop_score, 2), '%', ' ', '(', year, ')') AS cooperation_score_annual,
			CASE WHEN coop_score >= 99 THEN
				'A+'
			WHEN coop_score < 99
				AND coop_score >= 80 THEN
				'A'
			WHEN coop_score < 80
				AND coop_score >= 60 THEN
				'B'
			WHEN coop_score < 60
				AND coop_score >= 40 THEN
				'C'
			WHEN coop_score < 40
				AND coop_score >= 20 THEN
				'D'
			ELSE
				'F'
			END AS year_grade ,
      ROUND(((ROUND(SUM(total_distance_nm_under_10) OVER (PARTITION BY operator, vsr_region ORDER BY operator, year), 2) /ROUND(SUM(total_distance_nm) OVER (PARTITION BY operator, vsr_region ORDER BY operator, year), 2))*100), 2) AS rolling_coop_score,
      year_ship_count,
			ROUND(total_distance_nm, 2) AS total_distance_nm,
     	ROUND(SUM(total_distance_nm) OVER (PARTITION BY operator, vsr_region ORDER BY operator, year), 2) AS cum_sum_total_distance_nm,
			ROUND(total_distance_nm_under_10, 2) AS total_distance_nm_under_10,
      ROUND(SUM(total_distance_nm_under_10) OVER (PARTITION BY operator, vsr_region ORDER BY operator, year), 2) AS cum_sum_total_distance_nm_under_10,
			ROUND(total_distance_nm_btwn_10_12, 2) AS total_distance_nm_btwn_10_12,
			ROUND(total_distance_nm_btwn_12_15, 2) AS total_distance_nm_btwn_12_15,
			ROUND(total_distance_nm_over_15, 2) AS total_distance_nm_over_15,
			ROUND(avg_speed_knots, 2) AS avg_speed_knots,
			ship_categories_list,
			mmsi_list,
			name_of_ships,
      max_timestamp ,
      min_timestamp ,
      include_exclude_category AS exclude_category
		FROM (
			SELECT
				*,
				(total_distance_nm_under_10 / total_distance_nm) * 100 AS coop_score,
			FROM (
				SELECT
					operator,
					operator_code,
          vsr_region,
					EXTRACT(year FROM date) AS year,
					COUNT(DISTINCT (mmsi)) AS year_ship_count,
					SUM(total_distance_nm) AS total_distance_nm,
					SUM(total_distance_nm_under_10) AS total_distance_nm_under_10,
					SUM(total_distance_nm_btwn_10_12) AS total_distance_nm_btwn_10_12,
					SUM(total_distance_nm_btwn_12_15) AS total_distance_nm_btwn_12_15,
					SUM(total_distance_nm_over_15) AS total_distance_nm_over_15,
					(SUM(avg_speed_knots)/COUNT(day_count)) AS avg_speed_knots,
          MAX( max_timestamp ) AS max_timestamp ,
          MIN( min_timestamp ) AS min_timestamp ,
					STRING_AGG(DISTINCT(CAST(mmsi AS STRING)), ', ') AS mmsi_list,
					STRING_AGG(DISTINCT (shiptype), ', ') AS ship_types,
					STRING_AGG(DISTINCT (name_of_ship), ', ') AS name_of_ships,
					STRING_AGG(DISTINCT (ship_category), ', ') AS ship_categories_list,
					CASE WHEN COUNT(DISTINCT (ship_category)) > 1 THEN
						'multi-category'
					WHEN COUNT(DISTINCT (ship_category)) = 1 THEN
						STRING_AGG(DISTINCT (ship_category))
					END AS operator_ship_categories,
          CASE WHEN
          COUNT(DISTINCT (exclude_category)) = 1
          AND
          STRING_AGG(DISTINCT(CAST(exclude_category AS STRING)), '') LIKE '%0%'
          THEN
						0
          WHEN
          COUNT(DISTINCT (exclude_category)) > 1
          AND STRING_AGG(DISTINCT(CAST(exclude_category AS STRING)), '') LIKE '%01%'
          THEN
						0
          WHEN
          COUNT(DISTINCT (exclude_category)) > 1
          AND STRING_AGG(DISTINCT(CAST(exclude_category AS STRING)), '') LIKE '%10%'
          THEN
						0
          WHEN
          COUNT(DISTINCT (exclude_category)) = 1
          AND
          STRING_AGG(DISTINCT(CAST(exclude_category AS STRING)), '') LIKE '%1%'
          THEN
            1
					END AS include_exclude_category
          FROM
					`whalesafe_v3.ship_stats_daily`
          WHERE vsr_category NOT LIKE '%off%'
				GROUP BY
					operator,
					operator_code,
          vsr_region,
					year
          )
          )
          ;

-- # -- Step 2: Create a timestamp log to track newest timestamps in stats data.
CREATE TABLE IF NOT EXISTS `whalesafe_v3.stats_log` (
      newest_timestamp TIMESTAMP,
      newest_date DATE,
			date_accessed TIMESTAMP,
			table_name STRING);

-- # -- Step 3: Insert newest timestamp into log
INSERT INTO `whalesafe_v3.stats_log`
SELECT
	(
		SELECT
			MAX(max_timestamp)
		FROM
			`whalesafe_v3.operator_stats_annual`
		LIMIT 1) AS newest_timestamp,
    (
		SELECT
			DATE(MAX(max_timestamp))
		FROM
			`whalesafe_v3.operator_stats_annual`
		LIMIT 1) AS newest_date,
	CURRENT_TIMESTAMP() AS date_accessed,
	'operator_stats_annual' AS table_name;
