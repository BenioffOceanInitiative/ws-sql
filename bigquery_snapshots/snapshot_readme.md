# WhaleSafe V3 BigQuery Snapshots 

This folder documents the sql queries used to schedule the snapsots of the `whalesafe_v3` table in the Benioff Ocean Initiative BigQuery database. 

The `whalesafe-snapshot-bot` service account runs the scheduled queries. 

Five tables from `whalesafe_v3` are queried weekly, monthly and yearly: 

1. `ais_data`
2. `ais_segments`
3. `ais_vsr_segments` 
4. `ihs_data_all` 
5. `ship_stats_daily`

The snapshots of these tables are saved in the `whalesafe_v3_backup` table. The expiration for the weekly snapshots is 40 days, monthly snapshots is 366 days and the yearly snapshots never expire. The weekly snapshots are scheduled to run every Monday at 5:00, the monthly ones every 1st of the month at 5:00 and the yearly ones on May 1st at 5:00 annually, setting up for the beginning of the vsr seasons. 

These scheduled queries were executed using the BigQuery cloud console following the guidelines provided [here](https://cloud.google.com/bigquery/docs/table-snapshots-scheduled). 

Earch query was prefaced with information like the following, where the display name and schedule can be edited for each query.  

```
bq query 
--use_legacy_sql=false 
--display_name="Monthly snapshots of the ais_data table" \
--location="us" 
--schedule="1 of month 05:00" \
--project_id=benioff-ocean-initiative \

```

This was then followed by the queries in this folder. After completing the scheduling of the queries, the project has to be transferred to the service account using the following two queries. 

```
bq ls --transfer_config=true 
--transfer_location=us
```

The output of this query is a list of the projects previously run. 

The following query is run with the most recent project from the list outputted replacing the last line. 

```
bq update --transfer_config 
--update_credentials \
--service_account_name=whalesafe-snapshot-bot@benioff-ocean-initiative.iam.gserviceaccount.com \
projects/12345/locations/us/transferConfigs/12345
```

