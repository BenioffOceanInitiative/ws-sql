import pandas as pd
from google.cloud  import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest, retry
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
# bq_tbl_gfw     = "benioff-ocean-initiative.whalesafe_v4.gfw_spire_daily_region"

# messages_scored_table                     = "world-fishing-827.pipe_production_v20201001.messages_scored_"
# research_satellite_timing_table           = "world-fishing-827.gfw_research.pipe_v20201001_satellite_timing"
# static_sunrise_dataset_and_table          = "world-fishing-827.pipe_static.sunrise"
# static_norad_to_receiver                  = "world-fishing-827.pipe_static.norad_to_receiver_v20200127"
# satellite_positions_one_second_resolution = "world-fishing-827.satellite_positions_v20190208.satellite_positions_one_second_resolution_"
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

path_gfw_pts_sql  = "sql_v4/gfw_pts.sql"
path_ais_data_sql = "sql_v4/ais_data.sql"

def msg(txt):
  #txt = '  158.208 seconds; expected completion: 2020-06-09 12:48:31.328176-07:00'
  print(txt + " ~ " + datetime.now(tz.gettz('America/Los_Angeles')).strftime('%Y-%m-%d %H:%M:%S PDT'))
  sys.stdout.flush()

def sql_fmt(f):
  return(open(f, "r").read().format(**dict(globals(), **locals())))

#date = date.today() - timedelta(7)
#date = datetime(2017, 1, 1).date()
# f'{date.today() - timedelta(8)}' # '2021-10-13'
# f'{date.today() - timedelta(7)}' # '2021-10-14'

# res = bq_client.query(
#   sql_fmt("sql_v4/create_gfw-daily.sql"),
#   job_id_prefix = 'create_gfw-daily_').result()

# date_beg = date(2017,  1, 1)
# date_beg = date(2017,  1, 15)
date_beg = date(2017,  3,  7)
date_end = date(2021, 10, 26)
#date_end = date(2017, 1, 3)
delta = date_end - date_beg
n_days = delta.days + 1

df_rgns = bq_client.query(f"SELECT rgn, ST_Extent(geog) AS bbox FROM {dataset}.rgns GROUP BY rgn ORDER BY rgn").to_dataframe()
n_rgns = df_rgns.shape[0]

n_jobs = n_days * n_rgns

@retry.Retry(initial=3, maximum=60*20, multiplier=2, deadline=60*20*5)
def retry_bq_job(job, job_pfx):
  msg(f'trying {job_pfx}')
  return job.result() # wait for result

for i_day in range(n_days): # i_day = 0
  date = date_beg + timedelta(days=i_day)
  # date = date(2021, 1, 1)
  #print(day)
  msg(f'day {i_day+1} of {n_days}: {date}')
  
  for i_rgn,row in df_rgns.iterrows(): # i_rgn = 1; row = df_rgns.loc[i_rgn,]
    rgn = row['rgn']
    xmin, xmax, ymin, ymax = [row['bbox'][key] for key in ['xmin', 'xmax', 'ymin', 'ymax']]
    i_job = (i_day * n_rgns) + (i_rgn + 1)
    
    job_pfx = f'{date}_{rgn}_gfw_pts_'
    msg(f'  job {i_job} of {n_jobs}: {job_pfx}')
    sql = sql_fmt(path_gfw_pts_sql)
    # print(sql)
    job = bq_client.query(sql, job_id_prefix = job_pfx)
    # Will retry flaky_rpc() if it raises transient API errors.
    result = retry_bq_job(job, job_pfx)

    try:
      job.result()
    # except BadRequest:
    #   for e in job.errors:
    #     print('ERROR: {}'.format(e['message']))
    except HttpError, err:
    # If the error is a rate limit or connection error, wait and
    # try again.
    # 403: Forbidden: Both access denied and rate limits.
    # 408: Timeout
    # 500: Internal Service Error
    # 503: Service Unavailable
    if err.resp.status in [403, 408, 500, 503]:
      print '%s: Retryable error %s, waiting' % (
          self.thread_id, err.resp.status,)
      time.sleep(5)
    else: raise
    #time.sleep(5)

# n_jobs = 10
print(f"Last {n_jobs} jobs:\n              begin |                 end | status | name                                 | errors")
for job in bq_client.list_jobs(max_results=n_jobs):  # API request(s)
    print(f"{job.created:%Y-%m-%d %H:%M:%S} | {job.state} | {job.job_id} | {job.exception()}")

err = help(job.exception)();
Your table exceeded quota for imports or query appends per table

#   job 207 of 6984: 2017-03-07_USA-GoMex_gfw_pts_ ~ 2021-10-27 17:33:53 PDT
# Error in py_run_file_impl(file, local, convert) : 
#   BadRequest: 400 GET https://bigquery.googleapis.com/bigquery/v2/projects/benioff-ocean-initiative/queries/2017-03-07_USA-GoMex_gfw_pts_62aefee9-eb0b-47a9-9a61-1cb696ceb4bd?maxResults=0&location=US: Quota exceeded: Your table exceeded quota for imports or query appends per table. For more information, see https://cloud.google.com/bigquery/troubleshooting-errors at [99:1]


# # 2. ais_data
# path_ais_data_sql= "sql_v4/ais_data.sql"
# job_pfx = f'ais_data_'
# msg(job_pfx)
# sql = sql_fmt(path_ais_data_sql)
# #print(sql)
# bq_client.query(sql, job_id_prefix = job_pfx)
# 
# # 3. ais_segments
# path_ais_data_sql= "sql_v4/ais_segments.sql"
# job_pfx = f'{date}_{rgn}_ais_data_'
# msg(job_pfx)
# sql = sql_fmt(path_ais_data_sql)
# #print(sql)
# bq_client.query(sql, job_id_prefix = job_pfx)
