# prerequisites: Ben on new MacBook Air
# - [Visual Studio Code](https://code.visualstudio.com/download)
# - [homebrew](https://brew.sh)
# - [Postgres app for Mac](https://postgresapp.com/downloads.html)
# vi ~/.zprofile
#   PATH=$PATH:/Users/bbest/Library/Python/3.8/bin:/Applications/Postgres.app/Contents/Versions/latest/bin
# /opt/homebrew/bin/pip3 install --upgrade pandas google-cloud-bigquery pyarrow sqlalchemy psycopg2 python-dateutil oauth2client google-auth-httplib2 google-auth-oauthlib google-api-python-client

# modules
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
from __future__ import print_function
import json
from google.auth.transport.requests import Request
#from google.oauth2.credentials import Credentials
from oauth2client.service_account import ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# googlesheet variables
# TODO: spreadsheets read AND WRITE for zone_dates
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly', 
    'https://www.googleapis.com/auth/drive'] # 'https://spreadsheets.google.com/feeds',
# [zones - Google Sheets](https://docs.google.com/spreadsheets/d/1DnE1RY7exhRzc-e3kd8sX9HKRbjEvBHS4aZFXFLupeM/edit#gid=423793051)
SPREADSHEET_ID = '1DnE1RY7exhRzc-e3kd8sX9HKRbjEvBHS4aZFXFLupeM'
CREDENTIALS_JSON = '/Volumes/GoogleDrive/My Drive/projects/whalesafe/data/benioff-ocean-initiative-0b09860e2d00.json'
# lgnd-website-service-account: https://console.cloud.google.com/iam-admin/serviceaccounts/details/114569616080626900590;edit=true?previousPage=%2Fapis%2Fcredentials%3Fproject%3Dbenioff-ocean-initiative%26authuser%3D1&authuser=1&project=benioff-ocean-initiative
# shared Gsheet with ships4whales@benioff-ocean-initiative.iam.gserviceaccount.com as Editor

# dates
date_init = date(2017,  1,  1)
date_end  = date.today()

# bigquery connection
project_id       = 'benioff-ocean-initiative'
dataset          = 'whalesafe_v4'
#credentials_json = '/home/admin/Benioff Ocean Initiative-454f666d1896.json'
#credentials_json = '/Users/bbest/My Drive (ben@ecoquants.com)/projects/whalesafe/data/gfw/Benioff Ocean Initiative-454f666d1896.json'
credentials_json = '/Volumes/GoogleDrive/My Drive/projects/whalesafe/data/gfw/Benioff Ocean Initiative-454f666d1896.json'
# lgnd-website-service-account: https://console.cloud.google.com/iam-admin/serviceaccounts/details/114569616080626900590;edit=true?previousPage=%2Fapis%2Fcredentials%3Fproject%3Dbenioff-ocean-initiative%26authuser%3D1&authuser=1&project=benioff-ocean-initiative
credentials      = service_account.Credentials.from_service_account_file(credentials_json)
bq_client        = bigquery.Client(credentials=credentials, project=project_id)

tbl_gfw_messages_scored                           = "world-fishing-827.pipe_production_v20201001.messages_scored_"
tbl_gfw_research_satellite_timing                 = "world-fishing-827.gfw_research.pipe_v20201001_satellite_timing"
tbl_gfw_static_sunrise                            = "world-fishing-827.pipe_static.sunrise"
tbl_gfw_static_norad_to_receiver                  = "world-fishing-827.pipe_static.norad_to_receiver_v20200127"
tbl_gfw_satellite_positions_one_second_resolution = "world-fishing-827.satellite_positions_v20190208.satellite_positions_one_second_resolution_"

tbl_gfw_segs = "world-fishing-827.gfw_research.pipe_v20201001_segs"
tbl_rgn_pts  = "benioff-ocean-initiative.whalesafe_v4.rgn_pts"
tbl_rgn_segs = "benioff-ocean-initiative.whalesafe_v4.rgn_segs"
tbl_shore    = "benioff-ocean-initiative.whalesafe_v4.shore"
tbl_rgns     = "benioff-ocean-initiative.whalesafe_v4.rgns"
tbl_zones    = "benioff-ocean-initiative.whalesafe_v4.zones"
tbl_ais_data = "benioff-ocean-initiative.whalesafe_v4.ais_data" # TODO: rename to zone_pts?
tbl_log      = "benioff-ocean-initiative.whalesafe_v4.timestamp_log"

# path_rgn_pts_sql      = "sql_v4/rgn_pts.sql"
# path_ais_data_sql     = "sql_v4/ais_data.sql"
# path_ais_segments_sql = "sql_v4/ais_segments.sql"

def msg(txt):
  print(txt + " ~ " + datetime.now(tz.gettz('America/Los_Angeles')).strftime('%Y-%m-%d %H:%M:%S PDT'))
  sys.stdout.flush()

# function to replace variables in the SQL scripts
def sql_fmt(f):
  if os.path.exists(f):
    return(open(f, "r").read().format(**dict(globals(), **locals())))
  else:
    return(f.format(**dict(globals(), **locals())))

def get_sheet(RANGE_NAME = "zones_spatial", SPREADSHEET_ID=SPREADSHEET_ID, CREDENTIALS_JSON=CREDENTIALS_JSON):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None

    with open(CREDENTIALS_JSON, 'r') as file:
      CREDENTIALS_STR= file.read().replace('\n', '')
    SHEETS_KEY = json.loads(CREDENTIALS_STR)
    
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(CREDENTIALS_JSON):
        creds = ServiceAccountCredentials.from_json_keyfile_dict(SHEETS_KEY, SCOPES)
    if not creds or creds.invalid:
        sys.exit("CREDENTIALS_JSON not found or invalid:" + CREDENTIALS_JSON)
    try:
      service = build('sheets', 'v4', credentials=creds)
      
      # Call the Sheets API
      sheet = service.spreadsheets()
      # RANGE_NAME = 'zone_dates'
      result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME).execute()
      values = result.get('values', [])
      
      if not values:
        print('No data found.')
        return()
      else:
        df = pd.DataFrame(values)
        fld_names = df.iloc[0] # grab the first row for the header
        df = df[1:]            # take the data less the header row
        df.columns = fld_names # set the header row as the df header
        return df
      
    except HttpError as err:
      print(err)

df_zones_spatial = get_sheet("zones_spatial")
df_zones_dates   = get_sheet("zones_dates")
df_speedbins     = get_sheet("speedbins")

# gets regions with last fetched date from GFW data, based on {tbl_rgn_pts}
df_rgns = bq_client.query(f"""
  SELECT r.*, date_max FROM 
  ((SELECT rgn, ST_Extent(geog) AS bbox 
   FROM `{tbl_rgns}`
   GROUP BY rgn) r
  LEFT JOIN
    (SELECT rgn, MAX(DATE(timestamp)) AS date_max
     FROM `{tbl_rgn_pts}` 
     GROUP BY rgn) p ON r.rgn = p.rgn)
  ORDER BY rgn
  """).to_dataframe()
n_rgns = df_rgns.shape[0]

# get zones with last analyzed date, based on {tbl_ais_data}
df_zones = bq_client.query(f"""
  SELECT z.*, date_max FROM 
  ((SELECT * EXCEPT (geog) 
    FROM {tbl_zones} ORDER BY rgn, zone) z
   LEFT JOIN
    (SELECT zone, (MAX(DATE(TIMESTAMP))) AS date_max 
     FROM `{tbl_ais_data}`
     WHERE DATE(TIMESTAMP) >= '{date_init}'
     GROUP BY zone) a ON z.zone = a.zone)
  ORDER BY rgn, zone
  """).to_dataframe()
n_zones = df_zones.shape[0]

msg(f'Iterating over {n_rgns} regions')

for i_rgn,row in df_rgns.iterrows(): # i_rgn = 0; row = df_rgns.loc[i_rgn,]
  rgn = row['rgn']
  xmin, xmax, ymin, ymax = [row['bbox'][key] for key in ['xmin', 'xmax', 'ymin', 'ymax']]
  date_beg = row['date_max']

  # rgn_pts
  job_pfx = f'rgn_pts_{rgn}_{date_beg}_{date_end}_'
  msg(f'rgn {i_rgn+1} of {n_rgns}: {job_pfx}')
  sql = sql_fmt('sql_v4/rgn_pts.sql') # print(sql)
  # f = open(f'sql_v4/rgn_pts_{rgn}_{date_beg}_{date_end}.sql', 'w')
  # f.write(sql); f.close()
  job = bq_client.query(sql, job_id_prefix = job_pfx)
  result = job.result() # uncomment to run

  # rgn_segs
  # date_beg = date(2017,  1,  1); date_end = date(2017,  3,  1)
  job_pfx = f'ais_data_{rgn}_{date_beg}_{date_end}_'
  msg(f'rgn {i_rgn+1} of {n_rgns}: {job_pfx}')
  sql = sql_fmt('sql_v4/rgn_segs.sql')
  f = open(f'sql_v4/rgn_segs_{rgn}_{date_beg}_{date_end}.sql', 'w')
  f.write(sql); f.close()
  job = bq_client.query(sql, job_id_prefix = job_pfx)
  result = job.result() # uncomment to run

  # # TODO: ais_segments_sql.sql for regions? or just zones? 
  # # - by rgn/zone or all at once?
  # # - load zones first
  # job_pfx = f'ais_segments_{rgn}_{date_beg}_{date_end}_'
  # msg(f'rgn {i_rgn+1} of {n_rgns}: {job_pfx}')
  # sql = sql_fmt(path_ais_segments_sql)
  # # f = open(f'{path_ais_segments_sql}_{rgn}_{date_beg}_{date_end}.sql', 'w')
  # # f.write(sql); f.close()
  # job = bq_client.query(sql, job_id_prefix = job_pfx)
  # # result = job.result() # uncomment to run

msg(f'Iterating over {n_zones} zones.')

for i_zone,row in df_zones.iterrows(): # i_zone = 0; row = df_zones.loc[i_zone,]
  rgn      = row['rgn']
  zone     = row['zone']
  date_beg = row['date_max']
  if date_beg == None:
    date_beg = date_init

  d_zone_spatial = df_zones_spatial[df_zones_spatial["zone"] == zone]
  d_zone_dates   = df_zones_dates[df_zones_dates["zone"] == zone]
  
  job_pfx = f'ais_data_{rgn}_{zone}_{date_beg}_{date_end}_'
  msg(f'{i_zone+1} of {n_zones}: region_zone {rgn}_{zone}: {job_pfx}')
  sql = sql_fmt(path_ais_data_sql) # print(sql)
  job = bq_client.query(sql, job_id_prefix = job_pfx)
  result = job.result() # uncomment to run

# TODO: ais_segments_sql.sql 
  # - by rgn/zone or all at once?
  # - load zones first
  job_pfx = f'ais_segments_{rgn}_{date_beg}_{date_end}_'
  msg(f'rgn {i_rgn+1} of {n_rgns}: {job_pfx}')
  sql = sql_fmt(path_ais_segments_sql) # print(sql)
  # f = open(f'{path_ais_segments_sql}_{rgn}_{date_beg}_{date_end}.sql', 'w')
  # f.write(sql); f.close()
  job = bq_client.query(sql, job_id_prefix = job_pfx)
  # result = job.result() # uncomment to run



# show job status
n_jobs = n_rgns
print(f"Last {n_jobs} jobs:\n              begin |                 end | status | name                                 | errors")
for job in bq_client.list_jobs(max_results=n_jobs):  # API request(s)
  print(f"{job.created:%Y-%m-%d %H:%M:%S} | {job.state} | {job.job_id}") # " | {job.exception()}")

# get summary of regions in rgn_pts
sql = "SELECT rgn, \
  MIN(timestamp) AS min_timestamp, MAX(timestamp) AS max_timestamp, \
  MIN(lon) AS min_lon, MAX(lon) AS max_lon, \
  MIN(lat) AS min_lat, MAX(lat) AS max_lat, \
  COUNT(*) AS cnt \
  FROM whalesafe_v4.rgn_pts \
  GROUP BY rgn"
job = bq_client.query(sql, job_id_prefix = 'SUMMARIZE_rgn_pts_RGN_')
result = job.result() # uncomment to run
job.to_dataframe().to_csv("data/rgn_pts_summary.csv")

# jobs meta: careful BIG (284 MB) last time and not restricted by date
sql = "SELECT * FROM `benioff-ocean-initiative`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT \
  -- WHERE STARTS_WITH(job_id, 'rgn_pts_') \
  WHERE DATE(creation_time) = DATE('2021-11-12') \
  ORDER BY creation_time"
job = bq_client.query(sql, job_id_prefix = "JOBS_rgn_pts_")
result = job.result() # uncomment to run
job.to_dataframe().to_csv("data/rgn_pts_jobs.csv")

# job
sql = "SELECT state, total_bytes_processed, error_result FROM `benioff-ocean-initiative`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT \
  WHERE job_id = 'rgn_pts_USA-East_2017-01-01_2021-11-12_016cfdf0-dce7-4289-9c21-d4f0ecaef3fd'"
job = bq_client.query(sql, job_id_prefix = "JOBS_rgn_pts_")
result = job.result() # uncomment to run
job.to_dataframe()


