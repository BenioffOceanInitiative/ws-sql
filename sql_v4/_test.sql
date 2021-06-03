-- !preview conn=DBI::dbConnect(RSQLite::SQLite())

SELECT * EXCEPT(linestring, point)           -- exclude geography columns
FROM `benioff-ocean-initiative.whalesafe_v3.ais_segments`
WHERE DATE(timestamp) > (CURRENT_DATE() - 7) -- filter by timestamp
ORDER BY timestamp DESC
LIMIT 10;
