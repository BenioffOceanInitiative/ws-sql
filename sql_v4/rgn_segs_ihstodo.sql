--# add ihs column(s)
ALTER TABLE `whalesafe_v4.rgn_segs` 
  ADD COLUMN IF NOT EXISTS length_m NUMERIC,
  ADD COLUMN IF NOT EXISTS weight_gt NUMERIC,
  ADD COLUMN IF NOT EXISTS operator STRING; --# gross tonnage for union with ihs data
 

SELECT s.mmsi, s.date, COUNT(*) AS cnt
FROM `whalesafe_v4.rgn_segs` AS s
INNER JOIN 
  -- `whalesafe_v4.ihs_vessels_20220513` AS i
  `whalesafe_v4.ihs_vessels_20211123` AS i
   ON s.mmsi = i.mmsi
WHERE 
  DATE(timestamp) >= '2022-01-01' AND
  DATE(timestamp) <  '2022-02-01'
GROUP BY s.mmsi, s.date
ORDER BY cnt DESC;

-- mmsi = 366867690: most frequent rgn_segs for 2022-01-01
SELECT 
  _TABLE_SUFFIX
  -- DATE_DIFF(s.date, DATE i._TABLE_SUFFIX, DAY) as date_diff
FROM `whalesafe_v4.ihs_vessels_*` AS i
WHERE 
  mmsi = 366867690;

AND 




UPDATE `whalesafe_v4.rgn_segs` AS s
SET
  DATE_DIFF(s.date, DATE i._TABLE_SUFFIX, DAY)

  s.length_m  = 
  i.length_m,
  s.weight_gt = i.weight_gt,
  s.operator  = i.operator
FROM 
  `whalesafe_v4.rgn_segs` AS s
  LEFT JOIN ihs_vessels_* AS i
  ON 
    s.mmsi = i.mmsi
    _TABLE_SUFFIX
WHERE
  ms
length_m	FLOAT	NULLABLE		
tonnage_gt
 
-- rgn_segs
         FROM
          `{tbl_rgn_pts}` AS ais
          LEFT JOIN
          `whalesafe_v3.ihs_data_all` AS ihs_data
          ON ais.mmsi = ihs_data.mmsi AND 
            DATE(ais.timestamp) >= ihs_data.start_date AND 
            DATE(ais.timestamp) <= ihs_data.end_date

