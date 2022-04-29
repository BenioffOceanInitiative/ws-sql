-- Information on how to run this query in the cloud console can be found in the snapshot.md document.

DECLARE snapshot_name STRING;
 DECLARE expiration TIMESTAMP;
 DECLARE query STRING;
 SET expiration = DATE_ADD(@run_time, INTERVAL 366 DAY);
 SET snapshot_name = CONCAT("whalesafe_v3_backup.ais_data_monthly_",
   FORMAT_DATETIME("%Y%m%d", @run_date));
 SET query = CONCAT("CREATE SNAPSHOT TABLE ", snapshot_name,
   " CLONE benioff-ocean-initiative.whalesafe_v3.ais_data OPTIONS(expiration_timestamp=TIMESTAMP \"",
   expiration, "\");");
 EXECUTE IMMEDIATE query;