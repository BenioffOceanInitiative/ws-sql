-- !preview conn=con, parameters=list(nmax=5)

SELECT * EXCEPT(linestring, point)
FROM ais_segments
WHERE DATE(timestamp) > (CURRENT_DATE() - 7)
ORDER BY timestamp DESC
LIMIT @nmax;
