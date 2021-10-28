DELETE FROM `benioff-ocean-initiative.whalesafe_v4.gfw_daily_spireonly`
WHERE
    DATE(timestamp) = DATE('2021-10-14') AND
    msgid IN (
        SELECT msgid FROM
        `benioff-ocean-initiative.whalesafe_v4.gfw_daily_spireonly` AS pts
        LEFT JOIN `benioff-ocean-initiative.whalesafe_v4.regions` AS rgns
        ON pts.ws_region = rgns.region
        WHERE
            DATE(timestamp) = DATE('2021-10-14') AND
            NOT ST_COVERS(rgns.geog, pts.geog))

-- SELECT ST_COVERS(rgns.geog, pts.geog) AS pt_in_rgn
-- FROM `benioff-ocean-initiative.whalesafe_v4.gfw_daily_spireonly` AS pts
-- LEFT JOIN `benioff-ocean-initiative.whalesafe_v4.regions` AS rgns
-- ON pts.ws_region = rgns.region
-- WHERE DATE(timestamp) = DATE('2021-10-14')
-- LIMIT 100;
