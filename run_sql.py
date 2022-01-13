# libraries
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


  # pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

# dates
date_init = date(2017,  1,  1)
date_end  = date.today()

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
tbl_zones    = "benioff-ocean-initiative.whalesafe_v4.zones"
tbl_ais_data = "benioff-ocean-initiative.whalesafe_v4.ais_data"
tbl_log      = "benioff-ocean-initiative.whalesafe_v4.timestamp_log"

#path_gfw_pts_sql  = "sql_v4/gfw_pts.sql"
path_gfw_pts_sql      = "sql_v4/gfw_pts.sql"
path_ais_data_sql     = "sql_v4/ais_data.sql"
path_ais_segments_sql = "sql_v4/ais_segments.sql"

def msg(txt):
  #txt = '  158.208 seconds; expected completion: 2020-06-09 12:48:31.328176-07:00'
  print(txt + " ~ " + datetime.now(tz.gettz('America/Los_Angeles')).strftime('%Y-%m-%d %H:%M:%S PDT'))
  sys.stdout.flush()

def sql_fmt(f):
  if os.path.exists(f):
    return(open(f, "r").read().format(**dict(globals(), **locals())))
  else:
    return(f.format(**dict(globals(), **locals())))

delta = date_end - date_beg
n_days = delta.days + 1 # 132 days

df_rgns = bq_client.query(f"""
  SELECT r.*, date_max FROM 
  ((SELECT rgn, ST_Extent(geog) AS bbox 
   FROM `{tbl_rgns}`
   GROUP BY rgn) r
  LEFT JOIN
    (SELECT rgn, MAX(DATE(timestamp)) AS date_max
     FROM `{tbl_gfw_pts}` 
     GROUP BY rgn) p ON r.rgn = p.rgn)
  ORDER BY rgn
  """).to_dataframe()
n_rgns = df_rgns.shape[0]

df_zones = bq_client.query(f"""
  SELECT z.*, date_max FROM 
  ((SELECT * EXCEPT (geog) 
    FROM {tbl_zones} ORDER BY rgn, zone) z
   LEFT JOIN
    (SELECT zone, (MAX(DATE(TIMESTAMP))) AS date_max 
     FROM `{tbl_ais_data}`
     WHERE DATE(TIMESTAMP) >= '2017-01-01'
     GROUP BY zone) a ON z.zone = a.zone)
  ORDER BY rgn, zone
  """).to_dataframe()
n_zones = df_zones.shape[0]

msg(f'Iterating over {n_rgns} regions')

for i_rgn,row in df_rgns.iterrows(): # i_rgn = 0; row = df_rgns.loc[i_rgn,]
  rgn = row['rgn']
  xmin, xmax, ymin, ymax = [row['bbox'][key] for key in ['xmin', 'xmax', 'ymin', 'ymax']]
  date_beg = row['date_max']

  # gfw_pts.sql
  job_pfx = f'gfw_pts_{rgn}_{date_beg}_{date_end}_'
  msg(f'rgn {i_rgn+1} of {n_rgns}: {job_pfx}')
  sql = sql_fmt(path_gfw_pts_sql) # print(sql)
  # f = open(f'{path_gfw_pts_sql}_{rgn}_{date_beg}_{date_end}.sql', 'w')
  # f.write(sql); f.close()
  job = bq_client.query(sql, job_id_prefix = job_pfx)
  result = job.result() # uncomment to run

  # # TODO: ais_data.sql for regions? or just zones?
  # # - by rgn/zone or all at once?
  # # - load zones first
  # job_pfx = f'ais_data_{rgn}_{date_beg}_{date_end}_'
  # msg(f'rgn {i_rgn+1} of {n_rgns}: {job_pfx}')
  # sql = sql_fmt(path_gfw_pts_sql)
  # # f = open(f'{path_gfw_pts_sql}_{rgn}_{date_beg}_{date_end}.sql', 'w')
  # # f.write(sql); f.close()
  # job = bq_client.query(sql, job_id_prefix = job_pfx)
  # # result = job.result() # uncomment to run

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

  job_pfx = f'ais_data_{rgn}_{zone}_{date_beg}_{date_end}_'
  msg(f'{i_zone+1} of {n_zones}: region_zone {rgn}_{zone}: {job_pfx}')
  sql = sql_fmt(path_ais_data_sql) # print(sql)
  job = bq_client.query(sql, job_id_prefix = job_pfx)
  # result = job.result() # uncomment to run
  
  # DEBUG Query error: Scalar subquery produced more than one element at [44:1]
  sql = sql_fmt("""
    DECLARE new_ais_ts DEFAULT (SELECT SAFE_CAST('2016-12-31 12:59:59 UTC' AS TIMESTAMP));
    SELECT *
      FROM `{tbl_gfw_pts}`
      WHERE
        DATE(timestamp) > DATE(new_ais_ts) AND
        ST_COVERS(
          (SELECT geog
            FROM `{tbl_zones}`
            WHERE zone = '{zone}'),
          geog)""")
    print(sql)
  sql = sql_fmt("""
    SELECT rgn, zone
      FROM `{tbl_zones}`
      WHERE zone = '{zone}'""")
    print(sql)
  job = bq_client.query(sql, job_id_prefix = job_pfx)
  job.to_dataframe()
  result = job.result() # uncomment to run


# TODO: ais_segments_sql.sql 
  # - by rgn/zone or all at once?
  # - load zones first
  job_pfx = f'ais_segments_{rgn}_{date_beg}_{date_end}_'
  msg(f'rgn {i_rgn+1} of {n_rgns}: {job_pfx}')
  sql = sql_fmt(path_ais_segments_sql)
  # f = open(f'{path_ais_segments_sql}_{rgn}_{date_beg}_{date_end}.sql', 'w')
  # f.write(sql); f.close()
  job = bq_client.query(sql, job_id_prefix = job_pfx)
  # result = job.result() # uncomment to run



# show job status
n_jobs = n_rgns
print(f"Last {n_jobs} jobs:\n              begin |                 end | status | name                                 | errors")
for job in bq_client.list_jobs(max_results=n_jobs):  # API request(s)
  print(f"{job.created:%Y-%m-%d %H:%M:%S} | {job.state} | {job.job_id}") # " | {job.exception()}")

# get summary of regions in gfw_pts
sql = "SELECT rgn, \
  MIN(timestamp) AS min_timestamp, MAX(timestamp) AS max_timestamp, \
  MIN(lon) AS min_lon, MAX(lon) AS max_lon, \
  MIN(lat) AS min_lat, MAX(lat) AS max_lat, \
  COUNT(*) AS cnt \
  FROM whalesafe_v4.gfw_pts \
  GROUP BY rgn"
job = bq_client.query(sql, job_id_prefix = 'SUMMARIZE_gfw_pts_RGN_')
result = job.result() # uncomment to run
job.to_dataframe().to_csv("data/gfw_pts_summary.csv")

# jobs meta: careful BIG (284 MB) last time and not restricted by date
sql = "SELECT * FROM `benioff-ocean-initiative`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT \
  -- WHERE STARTS_WITH(job_id, 'gfw_pts_') \
  WHERE DATE(creation_time) = DATE('2021-11-12') \
  ORDER BY creation_time"
job = bq_client.query(sql, job_id_prefix = "JOBS_gfw_pts_")
result = job.result() # uncomment to run
job.to_dataframe().to_csv("data/gfw_pts_jobs.csv")

# job
sql = "SELECT state, total_bytes_processed, error_result FROM `benioff-ocean-initiative`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT \
  WHERE job_id = 'gfw_pts_USA-East_2017-01-01_2021-11-12_016cfdf0-dce7-4289-9c21-d4f0ecaef3fd'"
job = bq_client.query(sql, job_id_prefix = "JOBS_gfw_pts_")
result = job.result() # uncomment to run
job.to_dataframe()


