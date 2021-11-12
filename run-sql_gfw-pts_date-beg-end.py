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

tbl_gfw_messages_scored                           = "world-fishing-827.pipe_production_v20201001.messages_scored_"
tbl_gfw_research_satellite_timing                 = "world-fishing-827.gfw_research.pipe_v20201001_satellite_timing"
tbl_gfw_static_sunrise                            = "world-fishing-827.pipe_static.sunrise"
tbl_gfw_static_norad_to_receiver                  = "world-fishing-827.pipe_static.norad_to_receiver_v20200127"
tbl_gfw_satellite_positions_one_second_resolution = "world-fishing-827.satellite_positions_v20190208.satellite_positions_one_second_resolution_"

tbl_gfw_segs = "world-fishing-827.gfw_research.pipe_v20201001_segs"
tbl_gfw_pts  = "benioff-ocean-initiative.whalesafe_v4.gfw_pts"
tbl_rgns     = "benioff-ocean-initiative.whalesafe_v4.rgns"
tbl_ais_data = "benioff-ocean-initiative.whalesafe_v4.ais_data"
tbl_log      = "benioff-ocean-initiative.whalesafe_v4.timestamp_log"

#path_gfw_pts_sql  = "sql_v4/gfw_pts.sql"
path_gfw_pts_sql  = "sql_v4/gfw_pts_date-beg-end.sql"
path_ais_data_sql = "sql_v4/ais_data.sql"

def msg(txt):
  #txt = '  158.208 seconds; expected completion: 2020-06-09 12:48:31.328176-07:00'
  print(txt + " ~ " + datetime.now(tz.gettz('America/Los_Angeles')).strftime('%Y-%m-%d %H:%M:%S PDT'))
  sys.stdout.flush()

def sql_fmt(f):
  return(open(f, "r").read().format(**dict(globals(), **locals())))

date_beg = date(2017,  1,  1)
#date_end = date(2021, 10, 26)
date_end = date(2017, 5, 12)
delta = date_end - date_beg
n_days = delta.days + 1 # 132 days

df_rgns = bq_client.query(f"SELECT rgn, ST_Extent(geog) AS bbox FROM {dataset}.rgns GROUP BY rgn ORDER BY rgn").to_dataframe()
n_rgns = df_rgns.shape[0]
#n_jobs = n_days * n_rgns

msg(f'Iterating over {n_rgns} regions for a span of {n_days} days.')

for i_rgn,row in df_rgns.iterrows(): # i_rgn = 0; row = df_rgns.loc[i_rgn,]
  rgn = row['rgn']
  xmin, xmax, ymin, ymax = [row['bbox'][key] for key in ['xmin', 'xmax', 'ymin', 'ymax']]

  job_pfx = f'{rgn}_gfw_pts_'
  msg(f'rgn {i_rgn} of {n_rgns}: {job_pfx}')
  sql = sql_fmt(path_gfw_pts_sql)
  print(sql)
  job = bq_client.query(sql, job_id_prefix = job_pfx)
  result = job.result()
