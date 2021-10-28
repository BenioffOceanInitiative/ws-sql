library(reticulate)
py_install("pandas")
py_install("google-cloud-storage")
py_install("google-cloud-bigquery")
py_install("google-auth")
py_install("sqlalchemy")
py_install("psycopg2")

py_install("datetime")
py_install("dateutil")
py_install("time")

conda_cmd <- conda_list()$python[1] %>% stringr::str_replace("/python$","/conda")
system(glue::glue("{conda_cmd} update -n base -c defaults conda -y"))
# /Users/bbest/Library/r-miniconda/bin/python
