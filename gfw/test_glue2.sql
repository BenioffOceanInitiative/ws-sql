SELECT
  msgid,
  timestamp,
  seg_id,
  lat,
  lon
FROM
  `{{ messages_scored_table }}`
  WHERE _TABLE_SUFFIX = '{{ format(mdate, "%Y%m%d") }}'
  AND (receiver is null -- receiver is null is important,
                        -- otherwise null spire positions are ignored
    OR receiver in ('rORBCOMM000','rORBCOMM999')
    OR receiver not in (
      SELECT
        receiver
  FROM
    `{{ research_satellite_timing_table }}`
  WHERE _partitiontime = '{{ mdate }}')
  AND ABS(dt) > 60
))
AND lat < 90
AND lat > -90
AND lon < 180
LIMIT 10;
