import pandas as pd
from google.cloud  import bigquery
from google.oauth2 import service_account
from sqlalchemy    import create_engine
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import date, datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from dateutil import tz
import time
import sys
import subprocess
import os

# bigquery connection
project_id       = 'benioff-ocean-initiative'
dataset          = 'whalesafe_v4'
#credentials_json = '/home/admin/Benioff Ocean Initiative-454f666d1896.json'
credentials_json = '/Users/bbest/My Drive (ben@ecoquants.com)/projects/whalesafe/data/gfw/Benioff Ocean Initiative-454f666d1896.json'
# lgnd-website-service-account: https://console.cloud.google.com/iam-admin/serviceaccounts/details/114569616080626900590;edit=true?previousPage=%2Fapis%2Fcredentials%3Fproject%3Dbenioff-ocean-initiative%26authuser%3D1&authuser=1&project=benioff-ocean-initiative
credentials      = service_account.Credentials.from_service_account_file(credentials_json)
bq_client        = bigquery.Client(credentials=credentials, project=project_id)
bq_tbl_gfw       = "benioff-ocean-initiative.whalesafe_v4.gfw_daily_spireonly"

messages_scored_table                     = "world-fishing-827.pipe_production_v20201001.messages_scored_"
research_satellite_timing_table           = "world-fishing-827.gfw_research.pipe_v20201001_satellite_timing"
static_sunrise_dataset_and_table          = "world-fishing-827.pipe_static.sunrise"
static_norad_to_receiver                  = "world-fishing-827.pipe_static.norad_to_receiver_v20200127"
satellite_positions_one_second_resolution = "world-fishing-827.satellite_positions_v20190208.satellite_positions_one_second_resolution_"

def msg(txt):
  #txt = '  158.208 seconds; expected completion: 2020-06-09 12:48:31.328176-07:00'
  print(txt + " ~ " + datetime.now(tz.gettz('America/Los_Angeles')).strftime('%Y-%m-%d %H:%M:%S PDT'))
  sys.stdout.flush()

def sql_fmt(f):
  return(open(f, "r").read().format(**dict(globals(), **locals())))

date = date.today() - timedelta(7)
# f'{date.today() - timedelta(8)}' # '2021-10-13'
# f'{date.today() - timedelta(7)}' # '2021-10-14'

res = bq_client.query(
  sql_fmt("sql_v4/create_gfw-daily.sql"),
  job_id_prefix = 'create_gfw-daily_').result()

df_rgns = bq_client.query(f"SELECT region, ST_Extent(geog) AS bbox FROM {dataset}.regions GROUP BY region").to_dataframe()

for i,row in df_rgns.iterrows(): # i = 1; row = df_rgns.loc[i,]
  rgn = row['region']
  xmin, xmax, ymin, ymax = [row['bbox'][key] for key in ['xmin', 'xmax', 'ymin', 'ymax']]
  
  bq_client.query(
    #print(sql_fmt("sql_v4/insert_gfw-daily_spire-only_ws-rgn.sql")),
    sql_fmt("sql_v4/insert_gfw-daily_spire-only_ws-rgn.sql"),
    job_id_prefix = f'insert_gfw_{date}_{rgn}_')

print("Last 4 jobs:")
for job in bq_client.list_jobs(max_results=4):  # API request(s)
    print(f"{job.job_id} | {job.state} | {job.errors}")
# insert_gfw_2021-10-14_USA_East_bab95ee9-ff96-4ce8-a2e2-1e44e157ddd6 | RUNNING | None
# insert_gfw_2021-10-14_USA_GoMex_8ee5d12d-c2c0-41fe-b73c-aaf9dd548151 | RUNNING | None
# insert_gfw_2021-10-14_CAN_GoStLawrence_688a380e-1717-4259-ad09-4ccc67ac8b1e | RUNNING | None
# insert_gfw_2021-10-14_USA_West_b8d78044-7fb7-4a09-a625-98355e51c492 | RUNNING | None

r = bq_client.query(
  sql_fmt("sql_v4/ais_data_create-tables.sql"),
  job_id_prefix = f'ais_data_create-tables_').result()

# 1,939,649

r = bq_client.query(
  sql_fmt("sql_v4/ais_data.sql"),
  job_id_prefix = f'ais_data_').result()

