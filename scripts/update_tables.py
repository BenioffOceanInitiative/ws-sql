import json
import os
import string
import sys
from dateutil import tz
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# GOOGLE_APPLICATION_CREDENTIALS = json.loads(
#     os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))

project_id = 'benioff-ocean-initiative'
# credentials = service_account.Credentials.from_service_account_info(GOOGLE_APPLICATION_CREDENTIALS)
# credentials = service_account.Credentials.from_service_account_info(GOOGLE_APPLICATION_CREDENTIALS)
# bq_client = bigquery.Client(credentials=credentials, project=project_id)
bq_client = bigquery.Client( project=project_id)

# Update this table list when adding a new ihs table in BQ.
# Put the newer/more accurate lists at the top
tables = [
    "benioff-ocean-initiative.whalesafe_v3.ihs_data_2022_may",
    "benioff-ocean-initiative.whalesafe_v3.ihs_data_maersk_2021",
    "benioff-ocean-initiative.whalesafe_v3.ihs_data_2021_V2",
    "benioff-ocean-initiative.whalesafe_v3.ihs_data_2021",
    "benioff-ocean-initiative.whalesafe_v3.ihs_data_2020",
    "benioff-ocean-initiative.whalesafe_v3.ihs_data_2019"
]

def main():

    sql = "DROP TABLE IF EXISTS `benioff-ocean-initiative.whalesafe_v3.temp_ihs_data_all`"
    job = bq_client.query(sql)
    job.result()

    sql = f"CREATE TABLE `benioff-ocean-initiative.whalesafe_v3.temp_ihs_data_all` AS SELECT *  REPLACE(CAST(length AS FLOAT64) as length,CAST(registered_owner_code AS Integer) as registered_owner_code) FROM {tables[0]}"
    job = bq_client.query(sql)
    job.result()

    eval = True

    for table_name in tables[1:]:
      sql_file = "update_tables.sql"
      sql = open(sql_file, "r").read().format(accumulating_table_name = "`benioff-ocean-initiative.whalesafe_v3.temp_ihs_data_all`", older_table_name = f"`{table_name}`")

      e_sql = f'evaluated/{table_name}.sql'
      os.makedirs(os.path.dirname(e_sql), exist_ok=True)
      print(f'  writing evaluated sql: {e_sql}')
      e = open(e_sql, 'w'); e.write(sql); e.close()
      if eval:
        job = bq_client.query(sql, job_id_prefix = f'{table_name.split(".")[-1]}')
        job.result()

main()