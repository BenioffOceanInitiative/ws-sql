-- Create a new table that combines continuous segments of ownership from the new table for comparing against the original table segments
INSERT INTO {accumulating_table_name} WITH accumulating_table as (
  select 
    mmsi, 
    min(start_date) start_date, 
    max(end_date) end_date, 
    count(1) Combine_Count 
  FROM 
    (
      SELECT 
        *, 
        COUNTIF(
          IFNULL(isNew, TRUE)
        ) OVER win grp 
      FROM 
        (
          SELECT 
            mmsi, 
            start_date, 
            end_date, 
            start_date > LAG(end_date) OVER win + 1 isNew 
          FROM 
            {accumulating_table_name} WINDOW win AS (
              PARTITION BY mmsi 
              ORDER BY 
                start_date
            )
        ) WINDOW win AS (
          PARTITION BY MMSI 
          ORDER BY 
            start_date
        )
    ) 
  group by 
    mmsi, 
    grp
) 
SELECT 
  * 
FROM 
  (
    (
      SELECT 
        table1.* REPLACE(
          DATE_SUB(table2.start_date, INTERVAL 1 DAY) AS end_date, 
          CAST(table1.length AS FLOAT64) as length, 
          CAST(
            table1.registered_owner_code AS Integer
          ) as registered_owner_code
        ) 
      FROM 
        {older_table_name} table1, 
        accumulating_table table2 
      WHERE 
        table1.mmsi = table2.mmsi 
        AND (
          (
            --   Overlapping parts
            --   |----table1----|
            --           |----table2----|
            table1.start_date < table2.start_date 
            AND table1.end_date > table2.start_date 
            AND table1.end_date < table2.end_date
          )
        )
    ) 
    UNION ALL 
      (
        SELECT 
          table1.* REPLACE(
            DATE_ADD(table2.end_date, INTERVAL 1 DAY) AS start_date, 
            CAST(table1.length AS FLOAT64) as length, 
            CAST(
              table1.registered_owner_code AS Integer
            ) as registered_owner_code
          ) 
        FROM 
          {older_table_name} table1, 
          accumulating_table table2 
        WHERE 
          table1.mmsi = table2.mmsi 
          AND (
            (
              --   Overlapping parts
              --           |----table1----|
              --   |----table2----|
              table2.start_date < table1.start_date 
              AND table2.end_date > table1.start_date 
              AND table2.end_date < table1.end_date
            )
          )
      ) 
    UNION ALL 
      -- Begin Sandwich Union
      -- Part one
      -- Create new segment for beginning part of overlap
      --   |------table1------|
      --       |--table2--|
      --   |new|
      (
        SELECT 
          table1.* REPLACE(
            DATE_SUB(table2.start_date, INTERVAL 1 DAY) AS end_date, 
            CAST(table1.length AS FLOAT64) as length, 
            CAST(
              table1.registered_owner_code AS Integer
            ) as registered_owner_code
          ) 
        FROM 
          {older_table_name} table1, 
          accumulating_table table2 
        WHERE 
          table1.mmsi = table2.mmsi 
          AND (
            (
              --   Sandwiched
              --   |------table1------|
              --      |--table2--|
              table1.start_date < table2.start_date 
              AND table1.end_date > table2.end_date
            )
          )
      ) 
    UNION ALL 
      -- Part two
      -- Create new segment for beginning part of overlap
      --   |------table1------|
      --       |--table2--|
      --                  |new|
      (
        SELECT 
          table1.* REPLACE(
            DATE_ADD(table2.end_date, INTERVAL 1 DAY) AS start_date, 
            CAST(table1.length AS FLOAT64) as length, 
            CAST(
              table1.registered_owner_code AS Integer
            ) as registered_owner_code
          ) 
        FROM 
          {older_table_name} table1, 
          accumulating_table table2 
        WHERE 
          table1.mmsi = table2.mmsi 
          AND (
            (
              --   Sandwiched
              --   |------table1------|
              --      |--table2--|
              table1.start_date < table2.start_date 
              AND table1.end_date > table2.end_date
            )
          )
      ) -- End sandwich union
    UNION ALL 
      (
        -- Include any timespans outside of the range of the new table for any particular mmsi
        SELECT 
          table1.* REPLACE(
            CAST(table1.length AS FLOAT64) as length, 
            CAST(
              table1.registered_owner_code AS Integer
            ) as registered_owner_code
          ) 
        FROM 
          {older_table_name} table1, 
          (
            SELECT 
              DISTINCT mmsi, 
              MIN(table2.start_date) OVER (PARTITION BY mmsi) min_start_date, 
              MAX(table2.end_date) OVER (PARTITION BY mmsi) max_end_date 
            from 
              accumulating_table table2
          ) table2 
        WHERE 
          (
            table1.mmsi = table2.mmsi 
            AND (
              table1.end_date < min_start_date 
              OR table1.start_date > max_end_date
            )
          )
      ) 
    UNION ALL 
      (
        -- Add all timespans for mmsi which do not occur in the accumulating table
        SELECT 
          * REPLACE(
            CAST(length AS FLOAT64) as length, 
            CAST(registered_owner_code AS Integer) as registered_owner_code
          ) 
        FROM 
          {older_table_name} 
        WHERE 
          mmsi NOT IN (
            SELECT 
              mmsi 
            FROM 
              accumulating_table 
            GROUP BY 
              mmsi
          )
      )
  ) 
ORDER BY 
  start_date
