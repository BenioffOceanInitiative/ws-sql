-- # Benioff Ocean Initiative: 2021-02-25
-- # -- Step 0: Drop existing 'ihs_data_all' table.
DROP TABLE IF EXISTS 
`whalesafe_v3.ihs_data_all`;

-- # -- Step 1: Create Updated ihs_data_all table.
-- # -- Uncomment or add to UNION ALL ihs_data_yyyy tables to create `ihs_data_all`
-- # -- Be sure that newly added ihs_data_yyyy tables have same schema 
-- # -- AND start_date & end_date fields!!!

CREATE TABLE IF NOT EXISTS 
`whalesafe_v3.ihs_data_all` 
AS
SELECT *
REPLACE(
  CAST(registered_owner_code AS Integer) AS registered_owner_code,
  CAST(length AS FLOAT64) AS length)
FROM 
`benioff-ocean-initiative.whalesafe_v3.ihs_data_2022_may`
UNION ALL 
SELECT *
REPLACE(
  CAST(registered_owner_code AS Integer) AS registered_owner_code,
  CAST(length AS FLOAT64) AS length)
FROM 
`whalesafe_v3.ihs_data_2021_V2`
UNION ALL 
SELECT *
REPLACE(
  CAST(registered_owner_code AS Integer) AS registered_owner_code,
  CAST(length AS FLOAT64) AS length)
FROM 
`whalesafe_v3.ihs_data_2021`
UNION ALL 
SELECT *
REPLACE(
  CAST(registered_owner_code AS Integer) AS registered_owner_code,
  CAST(length AS FLOAT64) AS length)
FROM
`whalesafe_v3.ihs_data_2020`
UNION ALL 
SELECT *
REPLACE(
  CAST(registered_owner_code AS Integer) AS registered_owner_code,
  CAST(length AS FLOAT64) AS length)
FROM 
`whalesafe_v3.ihs_data_2019` 
