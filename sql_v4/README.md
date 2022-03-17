
## Sequence
Sequence of scripts, originally based on [Scheduled queries – BigQuery – Benioff Ocean Initi… – Google Cloud Platform](https://console.cloud.google.com/bigquery/scheduled-queries?authuser=3&project=benioff-ocean-initiative):

## Per Region
1. `gfw_pts.sql`: getting points from GFW\
  `rgn_pts.sql`: GFW -> `rgn_pts`
1. `ais_segments.sql`: make segments from points\
  `rgn_segs.sql`: `rgn_pts` -> `rgn_segs`
  `rgn_segs_speedbins.sql`: `rgn_segs.speedbin*`

  
## Per Zone
1. `ais_data.sql`: subset regional segments into zone\
   `ais_segments.sql`: make segments from points\
   `ais_vsr_segments.sql`: extract segments for zone\
  `zone_segs.sql`: `rgn_segs` -> `zone_segs`
1. `ship_stats_daily.sql`: combine segments with ownership data and create daily stats (core for rest)
1. `ship_stats_monthly.sql`: aggregate ship by month
1. `ship_stats_annual.sql`: aggregate ship by year
1. `operator_stats_daily.sql`: aggregate operator (across ships) by day
1. `operator_stats_monthly.sql`: aggregate operator (across ships) by month
1. `operator_stats_annual.sql`: aggregate operator (across ships) by year
1. `ais_segments_agg.sql`: aggregate segments (start,end for day) by region, segid, speedbin#, date -- used for viz purposes on WhaleSafe website

## Other

- xtra_helper-queries.sql
