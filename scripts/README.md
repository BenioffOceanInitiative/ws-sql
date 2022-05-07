This document describes the process for how to update the ihs_data_all table in BigQuery using the update_tables.py script.
For the old way which just unions all the tables refer to archive/ihs_data_all.sql

The update_tables.py script can input new date ranges as well as date ranges that conflict with date ranges in other tables, it will automatically fix the date ranges according to the heirarchy of the list. This means that a table listed higher will have it's ownership date ranges put in as is, whereas any conflicting date ranges from a table lower in the list will have it's date ranges modified accordingly.

For example:
tables = [
    Updated_table,
    Older_table
]


If the Updated table has an entry for the same mmsi that conflicts with an entry in the Older table it will look like this
Updated_table
MMSI             Operator                2021-04-01         2021-08-31
4949304194143     Waves LTD.               |------------------|

Older_table
MMSI             Operator          2021-01-01                       2021-12-31
4949304194143     Ship Co.           |--------------------------------|

Result
MMSI             Operator      2021-01-01  2021-03-31
4949304194143     Ship Co.           |-----|

                                         2021-04-01         2021-08-31
4949304194143     Waves LTD.               |------------------|

                                                        2021-09-01  2021-12-31
4949304194143     Ship Co.                                    |-----|

It can handle any type of overlap and update the date ranges accordingly.

Process Overview:
1. Retrieve mmsi list from company and fill in data using IHS site.
2. Upload table as new table in BigQuery under whalesafe_v3.
    If montly update, table name should be something like ihs_data_2022_may
    If update from company list, table name should be something like ihs_data_maersk_2021
3. Add the same table name to the list in update_tables.py at the top most likely, or atleast above any tables it should correct.
4. Open terminal or command prompt
5. Run `gcloud auth login` (Install gcloud cli if you don't have it https://cloud.google.com/sdk/docs/install)
6. Run the python script using `python update_tables.py`
7. Make sure it completes without error. It will output each query it runs as a sql file under the evaluated folder.