library(tidyverse)
library(data.table)

all_output_files = list.files('tableau/', pattern = "csv", full.names = TRUE)

Get_File_Date = function(fpath) {

  df_raw = fread(fpath)
  if ('Date' %in% names(df_raw)) {
    max_date = as.character(max(df_raw$Date, na.rm = TRUE))
  } else {
    max_date = NA_character_
  }

  output = list (
    fpath = fpath,
    max_date = max_date
  )

  return(output)
}


results_raw = map(all_output_files, Get_File_Date)

results = rbindlist(results_raw)



