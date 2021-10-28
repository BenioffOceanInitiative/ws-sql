-- delete all data
DELETE FROM whalesafe_v4.gfw_pts WHERE DATE(timestamp) > DATE('2000-01-01');

-- minimum timestamp of GFW
SELECT
DATE(MIN(timestamp)) AS date_beg
FROM `world-fishing-827.pipe_production_v20201001.messages_scored_*`
WHERE
source = 'spire';


-- update regions to hyphenated form, ran: 2021-10-27
UPDATE `benioff-ocean-initiative.whalesafe_v4.regions`
SET region = (CASE region
    WHEN 'USA_West'         THEN 'USA-West'
    WHEN 'CAN_GoStLawrence' THEN 'CAN-GoStLawrence'
    WHEN 'USA_GoMex'        THEN 'USA-GoMex'
    WHEN 'USA_East'         THEN 'USA-East'
    END)
WHERE TRUE;
