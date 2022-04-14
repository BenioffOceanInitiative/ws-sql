--# add ihs column(s)
ALTER TABLE `{tbl_rgn_segs}` 
  ADD COLUMN IF NOT EXISTS gt NUMERIC; --# gross tonnage for union with ihs data
 
 
-- rgn_segs
         FROM
          `{tbl_rgn_pts}` AS ais
          LEFT JOIN
          `whalesafe_v3.ihs_data_all` AS ihs_data
          ON ais.mmsi = ihs_data.mmsi AND 
            DATE(ais.timestamp) >= ihs_data.start_date AND 
            DATE(ais.timestamp) <= ihs_data.end_date

