if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}
shelf(
  bigrquery, DBI, dplyr, here) # connections

# ships4whales@benioff-ocean-initiative.iam.gserviceaccount.com
dir_auth_json <- switch(
  Sys.info()[["effective_user"]],
  calliesteffen = "/Volumes/GoogleDrive/My Drive/whalesafe/data/gfw",
  #bbest         = "/Volumes/GoogleDrive/My Drive/projects/whalesafe/data/gfw",
  bbest         = "/Users/bbest/My Drive/projects/whalesafe/data/gfw",
  rachelrhodes  = "/Volumes/GoogleDrive/.shortcut-targets-by-id/1crBGnOPGiKdWbtOLQhzgdJKA1ztZBzTM/gfw",
  cdobbelaere   = "~/TBD.json")
auth_json <- file.path(dir_auth_json, "Benioff Ocean Initiative-454f666d1896.json")
stopifnot(file.exists(auth_json))

bigrquery::bq_auth(path = auth_json)

# database connnection for Connections pane in RStudio
# con <<- connections::connection_open(
#   bigrquery::bigquery(),
#   project = "benioff-ocean-initiative",
#   # dataset = "whalesafe_v3",
#   dataset = "whalesafe_v4",
#   billing = "benioff-ocean-initiative",
#   use_legacy_sql = FALSE)
# connection_close(con)

# DBI only connection
con <<- dbConnect(
  bigrquery::bigquery(),
  project = "benioff-ocean-initiative",
  dataset = "whalesafe_v4",
  billing = "benioff-ocean-initiative")
