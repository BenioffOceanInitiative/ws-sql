-- !preview conn=con, params=list(messages_scored_table='messages_scored_', research_satellite_timing_table='world-fishing-827.gfw_research.pipe_v20201001_satellite_timing')

-- Source: world-fishing-827:pipe_production_v20201001.messages_scored_
-- * Satellite Timing: world-fishing-827:gfw_research.pipe_v20201001_satellite_timing
-- * Sunrise: world-fishing-827:pipe_static.sunrise
-- * Norad to Receiver: world-fishing-827:pipe_static.norad_to_receiver_v20200127
-- * Satellite positions one second resolution: world-fishing-827:satellite_positions_v20190208.satellite_positions_one_second_resolution_


-- CREATE TEMP FUNCTION toDAY() AS (DATE('{{ date }}'));
CREATE TEMP FUNCTION toDAY() AS (CURRENT_DATE('UTC'));
CREATE TEMP FUNCTION yesterDAY() AS (DATE_SUB(toDAY(), INTERVAL 1 DAY));
CREATE TEMP FUNCTION tomorrow() AS (DATE_ADD(toDAY(), INTERVAL 1 DAY));

SELECT
  msgid,
  timestamp,
  seg_id,
  lat,
  lon
FROM
  @messages_scored
  WHERE _TABLE_SUFFIX = YYYYMMDD( yesterDAY() )
  AND (receiver is null -- receiver is null is important,
                        -- otherwise null spire positions are ignored
    OR receiver in ('rORBCOMM000','rORBCOMM999')
    OR receiver not in (
      SELECT
        receiver
  FROM
    @research_satellite_timing_table
  WHERE _partitiontime = timestamp(yesterDAY())
  AND ABS(dt) > 60
))
AND lat < 90
AND lat > -90
AND lon < 180
LIMIT 10;
