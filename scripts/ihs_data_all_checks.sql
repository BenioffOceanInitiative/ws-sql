-- These queries should be run after running the ihs_data_all script

-- There should be no results for this query.
SELECT 
  * 
FROM 
  `benioff-ocean-initiative.whalesafe_v3.ihs_data_all` table1 
WHERE 
  EXISTS (
    SELECT 
      * 
    FROM 
      `benioff-ocean-initiative.whalesafe_v3.ihs_data_all` table2 
    WHERE 
      table1.mmsi = table2.mmsi 
      AND (
        (
          --   Overlapping parts
          --   |----table1----|
          --           |----table2----|
          table1.start_date < table2.start_date 
          AND table1.end_date > table2.start_date
        ) 
        OR (
          --   Sandwiched
          --   |------table1------|
          --      |--table2--|
          table1.start_date < table2.start_date 
          AND table1.end_date > table2.end_date
        )
      )
  )

-- Extended version of query above when checking against different tables
-- There should be no results for this query.
SELECT 
  * 
FROM 
  `benioff-ocean-initiative.whalesafe_v3.ihs_data_all` table1 
WHERE 
  EXISTS (
    SELECT 
      * 
    FROM 
      `benioff-ocean-initiative.whalesafe_v3.ihs_data_maersk_2021` table2 
    WHERE 
      table1.mmsi = table2.mmsi 
      AND (
        (
          --   Overlapping parts
          --   |----table1----|
          --           |----table2----|
          table1.start_date < table2.start_date 
          AND table1.end_date > table2.start_date
        ) 
        OR (
          --   Overlapping parts
          --           |----table1----|
          --   |----table2----|
          table2.start_date < table1.start_date 
          AND table2.end_date > table1.start_date
        ) 
        OR (
          --   Sandwiched
          --   |------table1------|
          --      |--table2--|
          table1.start_date < table2.start_date 
          AND table1.end_date > table2.end_date
        )
        OR (
          --   Sandwiched
          --        |--table1--|
          --   |------table2------|
          table2.start_date < table1.start_date 
          AND table2.end_date > table1.end_date
        )
      )
  )

-- Run this query and check that there are no overlapping dates
-- This runs under the assumption that all mmsi for each ihs table share the same start and end dates
-- If we do individual start and end dates for different mmsi, there will be overlapping results from this query 
SELECT start_date, end_date, count(*) as count, ARRAY_AGG(mmsi)
FROM 
  `benioff-ocean-initiative.whalesafe_v3.ihs_data_all`
 -- You can use this number to filter out any date ranges that only have a small number of occurances.
 -- Setting this number higher could be useful if you want to ignore some that have been updated individually.
 -- However, you could accidentally filter our false negatives if you do this. Better to filter out specific mmsi
 -- that you know have been filtered out mannually. The query above should catch that case.  
GROUP BY
  start_date,
  end_date
HAVING count(*) > 0
ORDER BY
  start_date,
  end_date
