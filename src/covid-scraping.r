# setup --------------------------------------------------------------------------------------------
library(tidyverse)
library(data.table)
library(readxl)
library(writexl)
library(stringr)
library(zoo)

library(lubridate)
library(ggpubr)

# web scraping
library(rvest)
library(jsonlite)
library(glue)

library(future)
library(furrr)
library(arrow)

select = dplyr::select
filter = dplyr::filter

# functions --------------------------------------------------------------------------------------------
# Grab every sheet from an excel file and convert to list of dataframes
# https://stackoverflow.com/questions/12945687/read-all-worksheets-in-an-excel-workbook-into-an-r-list-with-data-frames
read_excel_allsheets = function(filename, col_option = TRUE, add_date = FALSE, skip_option = 1) {
  sheets = openxlsx::getSheetNames(filename)

  x = map(sheets, ~openxlsx::read.xlsx(filename,
                                       sheet      = .,
                                       startRow   = skip_option + 1,
                                       colNames   = col_option,
                                       na.strings = '.')
  )

  names(x) = sheets
  x        = lapply(x, as.data.frame)

  if (add_date == TRUE) {
    file_date = str_extract(filename, '\\d.*\\d')
    x         = map(x, ~.x %>% mutate(Date = as.Date(file_date)))
  }
  return(x)
}

# set date for writing files
# If before 5 PM EST, then record as last date since DSHS data will not be updated yet
date_out = ifelse(
  (Sys.time() < as.POSIXct(paste0(Sys.Date(), '15:45'), tz = 'America/Chicago')),
  Sys.Date() - 1,
  Sys.Date()
)

# convert numeric sys.date() to yyyy-mm-dd
date_out = as.Date(date_out, origin = '1970-1-1')

Fix_Num_Dates = function(dates) {
  clean_dates = as.Date(as.numeric(dates), '1899-12-30')

  if (sum(is.na(clean_dates)) > 0) {
    clean_dates[which(is.na(clean_dates))] = as.Date(clean_dates[which(is.na(clean_dates))], origin = '1970-1-1')
  }
  return(format(clean_dates, '%Y-%m-%d'))
}

Fix_Char_Dates = function(dates) {
  #Matches 2 or 4 digits, separator, 2 or 4 digits, optional separator, optional 2 or 4 digits
  # Coerces all common separators to "-"
  date_regex  = '(\\d{4}|\\d{2}|\\d{1})(\\.|\\-|\\/)(\\d{4}|\\d{2}|\\d{1})?(\\.|\\-|\\/)?(\\d{4}|\\d{2})'
  clean_dates = str_extract(dates, date_regex) %>% str_replace_all(., '\\/|\\.', '\\-')

  mdy_dates = which(!is.na(as.Date(clean_dates, format = '%m-%d-%y')))
  md_dates  = which(is.na(as.Date(clean_dates, format = '%m-%d-%Y')))
  # if (length(mdy_dates) == length(md_dates)) {
  #   md_dates = c()
  # }


  if (length(md_dates > 0)) {
    md_dates_2020 = which(format(as.Date(clean_dates[md_dates], '%m-%d'), '%m') %in% as.character(seq(as.numeric(format(date_out, '%m')) + 1, 12)))

    if (length(md_dates_2020) > 0) {
      clean_dates[md_dates[md_dates_2020]]  = paste0('2020-', clean_dates[md_dates[md_dates_2020]])
      clean_dates[md_dates[-md_dates_2020]] = format(as.Date(clean_dates[md_dates[-md_dates_2020]],
                                                             '%m-%d'), '%Y-%m-%d')
    } else {
      clean_dates[md_dates] = format(as.Date(clean_dates, '%m-%d'), '%Y-%m-%d')
    }
  }

  if (length(mdy_dates > 0)) {
    clean_dates[mdy_dates] = format(as.Date(clean_dates[mdy_dates], '%m-%d-%Y'), '%Y-%m-%d')
  }

  return(clean_dates)
}

Date_Parser = function(raw_date) {
  numeric_loc = which(!is.na(as.numeric(raw_date)))

  if (length(numeric_loc) == 0) {
    clean_dates = Fix_Char_Dates(raw_date)
  } else {
    clean_num_date  = Fix_Num_Dates(raw_date[numeric_loc])
    clean_char_date = Fix_Char_Dates(raw_date[-numeric_loc])
    clean_dates     = sort(c(clean_num_date, clean_char_date))
  }
  return(clean_dates)
}

Download_Temp = function(url) {
  temp = tempfile()
  download.file(url, temp, mode = 'wb')
  return(temp)
}

# classifications --------------------------------------------------------------------------------------------
# TSA: https://www.dshs.texas.gov/sites/default/files/emstraumasystems/Trauma/pdf/TSAMap-RACNames.pdf
# Metro Area: https://www.dshs.state.tx.us/chs/info/TxCoPhrMsa.xls
# PHR: readable names from https://dshs.state.tx.us/regions/default.shtm
# Population: https://www.census.gov/data/tables/time-series/demo/popest/2020s-counties-total.html
county_metadata    = fread('tableau/helpers/county_metadata.csv')
county_names_vec = county_metadata$County
county_populations = county_metadata %>% select(County, Population_2020_04_01, Population_2020_07_01, Population_2021_07_01)


## -county demographics --------------------------------------------------------------------------------------------
# https://www.census.gov/data/datasets/time-series/demo/popest/2010s-counties-detail.html
# 2021 county level race estimates
# collapse into asian, black, hispanic, white, other
# exclude totals to avoid double counting
county_demo_agesex = fread('tableau/helpers/county_demo_agesex.csv')
county_demo_race   = fread('tableau/helpers/county_demo_race.csv')
state_demo_pops    = fread('tableau/helpers/state_demo.csv')

# wastewater --------------------------------------------------------------------------------------------

# houston dashboard --------------------------------------------------------------------------------------------
Scrape_Wastewater = function(data_type, offset = '') {
  plant_url = glue('https://services.arcgis.com/lqRTrQp2HrfnJt8U/ArcGIS/rest/services/WWTP_gdb/FeatureServer/0//query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset={offset}&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token=')
  zip_url   = glue('https://services.arcgis.com/lqRTrQp2HrfnJt8U/arcgis/rest/services/Wastewater_Zip_Case_Analysis/FeatureServer/0/query?where=1%3D1&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset={offset}&resultRecordCount=&sqlFormat=none&f=pjson&token=')

  url_selection = c('zip'   = zip_url,
                    'plant' = plant_url)

  ww_df = jsonlite::fromJSON(url_selection[[data_type]])[['features']][['attributes']]

  if (length(ww_df) > 0) {

    if (data_type == 'zip') {
      out_df = ww_df %>%
        mutate(across(contains('date'), function(x) as_datetime(x / 1000) %>% as_date())) %>%
        select(ZIPCODE, date, pop, contains('Spline'), contains('vax'))

      message(glue('{offset} max date: {max(out_df$date, na.rm = TRUE)}'))

    } else if (data_type == 'plant') {
      out_df = ww_df %>%
        mutate(across(contains('date'), function(x) as_datetime(x / 1000) %>% as_date())) %>%
        select(corname, date, vl_est, i_est_p, t_est, firstdate, lastdate, v1_est, v2_est, spline_ww) %>%
        rename(Plant            = corname,
               Date             = date,
               Viral_Load_PCT   = vl_est,
               p_value          = i_est_p,
               Trend            = t_est,
               Date_First       = firstdate,
               Date_Last        = lastdate,
               Viral_Copies_Log = spline_ww)
      message(glue('{offset} max date: {max(out_df$Date, na.rm = TRUE)}'))
    }
  } else {
    message(glue('{offset} returns no results'))
    return(list(NULL))
  }
  return(out_df)
}

plant_offsets = c('', seq(2000, 15000, by = 2000))
zip_offsets   = c('', seq(1000, 20000, by = 1000))

ww_plant_df = map(plant_offsets, ~Scrape_Wastewater('plant', offset = .)) %>%
  rbindlist(., fill = TRUE)
fwrite(ww_plant_df, 'tableau/wastewater_plant.csv')

ww_zip_df = map(zip_offsets, ~Scrape_Wastewater('zip', offset = .)) %>%
  rbindlist(., fill = TRUE)

ww_zip_df_clean = ww_zip_df %>%
  mutate(ZIPCODE = as.character(as.integer(ZIPCODE))) %>%
  filter(!is.na(ZIPCODE)) %>%
  arrange(ZIPCODE, date)

ww_zip_zipcode_n      = length(unique(ww_zip_df_clean$ZIPCODE))
ww_zip_missing_values = with(ww_zip_df_clean, table(ZIPCODE, date)) %>%
  as.data.frame() %>%
  pivot_wider(values_from = Freq, names_from = date) %>%
  filter(if_any(-one_of("ZIPCODE"), ~. != 1))

ww_zip_date_gaps = ww_zip_df_clean %>%
  select(date) %>%
  distinct() %>%
  arrange(date) %>%
  mutate(date_delta = date - lag(date)) %>%
  filter(!is.na(date_delta) & date_delta != 7)

ww_zip_checks = c(
  ww_zip_zipcode_n > 100,
  ww_zip_missing_values %>% nrow() == 0,
  ww_zip_date_gaps %>% nrow() == 0
)

if (all(ww_zip_checks)) {
  fwrite(ww_zip_df_clean, 'tableau/wastewater_zip.csv')
}

## cdc  --------------------------------------------------------------------------------------------
# cdc
cdc_ww       = fread('https://data.cdc.gov/api/views/2ew6-ywp6/rows.csv?accessType=DOWNLOAD')
cdc_ww_texas = cdc_ww %>%
  filter(wwtp_jurisdiction == 'Texas') %>%
  select(county_names, wwtp_id, sample_location, key_plot_id, date_start, ptc_15d, detect_prop_15d, percentile) %>%
  relocate(date_start, .before = 'county_names') %>%
  rename(Date        = date_start,
         County      = county_names,
         Location_ID = wwtp_id) %>%
  mutate(Date = format(Date, '%Y-%m-%d'))

fwrite(cdc_ww_texas, 'tableau/wastewater_county_cdc.csv')

# wastewater variants --------------------------------------------------------------------------------------------
# TODO: add values from https://covidwwtp.spatialstudieslab.org/

# COUNTY LEVEL --------------------------------------------------------------------------------------------

# # TPR cpr --------------------------------------------------------------------------------------------
# Split_TPR = function(all_results, cpr_date_results) {
#   cpr_dates_df      = rbindlist(cpr_date_results) %>%
#     mutate(cpr_date = as.Date(cpr_date, origin = '1970-01-01'))
#   cpr_dates_missing = cpr_dates_df %>% filter(is.na(cpr_date))
#   num_dates_missing = nrow(cpr_dates_missing)
#   message(glue('MISSING: {num_dates_missing} dates'))
#
#   # combine tpr archive --------------------------------------------------------------------------------------------
#   cpr_files_combined = all_results %>%
#     rbindlist(., fill = TRUE) %>%
#     left_join(cpr_dates_df, by = c('file_path' = 'cpr_file')) %>%
#     rename(TPR = TPR_CPR) %>%
#     mutate(TPR = ifelse(is.na(TPR), 0, TPR)) %>%
#     mutate(Tests = as.integer(Tests)) %>%
#     rename(Date = cpr_date) %>%
#     mutate(Date = as.Date(Date), origin = '1970-01-01') %>%
#     mutate(File_Date = as.Date(str_extract(file_path, '\\d{4}-\\d{2}-\\d{2}'), origin = '1970-01-01')) %>%
#     mutate(bad_date = Date > File_Date)
#   # mutate(Date = format(Date, '%Y-%m-%d'))
#
#   check_date = cpr_files_combined %>%
#     filter(bad_date) %>%
#     nrow() == 0
#
#   stopifnot(check_date)
#
#   cpr_files_split = cpr_files_combined %>%
#     select(Date, file_path, County, TPR, Tests, Transmission_Level, Community_Level) %>%
#     group_by(Date) %>%
#     group_split() %>%
#     purrr::set_names(purrr::map_chr(., ~format(.x$Date[1], '%Y-%m-%d')))
#
#   return(cpr_files_split)
# }
#
# Save_TPR_Results = function(cpr_files_split) {
#
#   walk(
#     names(cpr_files_split),
#     ~fwrite(cpr_files_split[[.]],
#             glue('original-sources/historical/cpr/cpr_tpr_{.}.csv'))
#   )
# }
#
# Get_CPR_Date = function(cpr_file) {
#   cpr_file_date = cpr_file %>%
#     str_extract_all(., '\\d{4}-\\d{2}-\\d{2}') %>%
#     unlist() %>%
#     as.Date()
#
#   cpr_sheets   = readxl::excel_sheets(cpr_file)
#   county_sheet = cpr_sheets[str_detect(str_to_lower(cpr_sheets), 'counties|county|count')]
#
#   tpr_names = suppressMessages(read_xlsx(cpr_file, sheet = county_sheet, skip = 0, n_max = 1)) %>%
#     select(contains('TESTING: LAST WEEK')) %>%
#     names()
#
#   cpr_year = cpr_file %>%
#     str_extract_all(., '\\d{4}') %>%
#     unlist() %>%
#     as.integer()
#
#   cpr_day = tpr_names %>%
#     str_match_all(., '(\\d{1,2})') %>%
#     .[[1]] %>%
#     .[nrow(.), ncol(.)] %>%
#     unlist() %>%
#     as.integer()
#
#   cpr_month = tpr_names %>%
#     str_extract_all(., 'January|February|March|April|May|June|July|August|September|October|November|December') %>%
#     as.data.frame() %>%
#     setNames('Date') %>%
#     mutate(Date = as.Date(glue('{Date}-01-{cpr_year}'), '%B-%d-%Y')) %>%
#     slice(nrow(.)) %>%
#     pull(Date) %>%
#     month()
#
#   cpr_date_result = tryCatch({
#     cpr_date = as.Date(glue('{cpr_year}-{cpr_month}-{cpr_day}'), '%Y-%m-%d')
#   },
#     error = function(e) {
#       cpr_date = NA
#     }
#   )
#
#   cpr_date = ifelse(
#     !is.na(cpr_date_result) & cpr_date_result > cpr_file_date,
#     cpr_date_result - years(1L),
#     cpr_date_result
#   )
#
#   output = list(
#     'cpr_file' = cpr_file,
#     'cpr_date' = cpr_date
#   )
#
#   return(output)
# }
#
# Clean_TPR = function(cpr_file) {
#   cpr_sheets   = readxl::excel_sheets(cpr_file)
#   county_sheet = cpr_sheets[str_detect(str_to_lower(cpr_sheets), 'counties|county|count')]
#
#   tpr_result = tryCatch(
#   {
#
#     cpr_tpr_raw = read_xlsx(cpr_file, sheet = county_sheet, skip = 1) %>%
#       filter(`State Abbreviation` == 'TX') %>%
#       mutate(County = str_replace_all(County, ' County, TX', '')) %>%
#       filter(County %in% county_metadata$County) %>%
#       setNames(str_to_lower(names(.))) %>%
#       setNames(str_replace_all(names(.), ' |-', ' ')) %>%
#       setNames(str_squish(names(.))) %>%
#       setNames(str_replace_all(names(.), ' ', '_'))
#
#     cpr_tpr = cpr_tpr_raw %>%
#       select(
#         any_of(
#           c(
#             matches('\\bcounty\\b'),
#             contains('naat_positivity_rate_last_7_days'),
#             contains('total_naats_last_7_days'),
#             contains('transmission_level_last_7_days'),
#             contains('community_level_last_7_days')
#           )
#         )
#       ) %>%
#       rename_with(
#         ~case_when(
#           str_detect(., "naat_positivity_rate_last_7_days") ~ "TPR_CPR",
#           str_detect(., "total_naats_last_7_days") ~ "Tests",
#           str_detect(., "transmission_level_last_7_days") ~ "Transmission_Level",
#           str_detect(., 'community_level_last_7_days') ~ "Community_Level",
#           TRUE ~ .
#         )
#       ) %>%
#       mutate(file_path = cpr_file) %>%
#       rename(County = county) %>%
#       relocate(file_path, .before = 'County') %>%
#       arrange(County)
#   },
#     error = function(e) {
#       message(e)
#       cpr_tpr = data.frame(file_path = cpr_file, County = NA_character_, TPR_CPR = NA_real_, Tests = NA_real_)
#       return(cpr_tpr)
#     }
#   )
#   return(tpr_result)
# }
#
# Download_TPR = function(cpr_url) {
#   tryCatch(
#   {
#     file_url = paste0('https://beta.healthdata.gov', cpr_url) %>%
#       str_replace_all(.,
#                       'https://beta.healthdata.govhttps://beta.healthdata.gov',
#                       'https://beta.healthdata.gov') %>%
#       str_replace_all(., ' ', '%20')
#
#     file_date = as.Date(str_match(cpr_url, '202\\d{5}'), '%Y%m%d')
#
#     download.file(file_url, glue('original-sources/historical/cpr_orig/cpr_{file_date}.xlsx'), mode = 'wb')
#   },
#     error = function(e) {
#       message(cpr_url)
#       message(e)
#     }
#   )
# }
#
# Check_Missing_TPR_Date = function(tpr_urls) {
#   all_tpr_url_dates = data.frame(tpr_url = tpr_urls) %>%
#     mutate(tpr_date_raw = str_extract(tpr_url, '202\\d{5}')) %>%
#     mutate(tpr_date = as.Date(tpr_date_raw, '%Y%m%d')) %>%
#     mutate(tpr_date = as.character(tpr_date))
#
#   all_tpr_downloads      = list.files('original-sources/historical/cpr_orig/', full.names = TRUE) %>%
#     .[!str_detect(., '\\~')]
#   all_tpr_download_dates = all_tpr_downloads %>%
#     str_extract_all(., '\\d{4}-\\d{2}-\\d{2}') %>%
#     unlist()
#
#   missing_dates = setdiff(all_tpr_url_dates %>% pull(tpr_date), all_tpr_download_dates)
#
#   missing_date_paths = all_tpr_url_dates %>%
#     filter(tpr_date %in% missing_dates) %>%
#     pull(tpr_url)
#
#   output = list(
#     'missing_dates'      = missing_dates,
#     'all_tpr_url_dates'  = all_tpr_url_dates,
#     'all_tpr_downloads'  = all_tpr_downloads,
#     'missing_date_paths' = missing_date_paths
#   )
#   return(output)
# }
#
# Manage_TPR_Download = function(tpr_urls, n_days) {
#   date_diff_results = Check_Missing_TPR_Date(tpr_urls)
#   missing_dates     = date_diff_results$missing_dates
#   new_files         = date_diff_results$missing_date_paths
#   max_attempts      = 2L
#   attempts          = 0L
#
#   while (length(missing_dates) > 0 & attempts < max_attempts) {
#     # plan(multicore, workers = parallel::detectCores())
#     furrr::future_walk(tpr_urls, ~Download_TPR(.))
#     # plan(sequential)
#     attempts = attempts + 1
#
#     date_diff_results = Check_Missing_TPR_Date(tpr_urls)
#     missing_dates     = date_diff_results$missing_dates
#     tpr_urls          = date_diff_results$missing_date_paths
#   }
#
#   if (attempts >= max_attempts) {
#     message('ERR: TOO MANY ATTEMPTS')
#   } else if (length(missing_dates) == 0) {
#     message('No missing dates')
#   }
#
#   output = list(
#     'new_files' = new_files,
#     'diff'      = date_diff_results
#   )
#
#   return(output)
# }
#
# Get_TPR_Urls = function(n_days = 1) {
#   page     = read_html('https://beta.healthdata.gov/National/COVID-19-Community-Profile-Report/gqxm-d9w9')
#   file_loc = page %>%
#     html_nodes('script') %>%
#     html_text() %>%
#     str_detect('\\.xlsx') %>%
#     which()
#
#   file_text = page %>%
#     html_nodes('script') %>%
#     .[file_loc] %>%
#     html_text() %>%
#     str_squish() %>%
#     str_replace_all(., 'var initialState =', '') %>%
#     str_replace_all(., '\\;', '') %>%
#     str_squish()
#
#   tpr_urls_raw = fromJSON(file_text)$view$attachments %>%
#     filter(href %>% str_detect('.xlsx')) %>%
#     distinct() %>%
#     arrange(desc(name))
#
#   n_days = ifelse(is.infinite(n_days), nrow(tpr_urls_raw), n_days)
#
#   tpr_urls = tpr_urls_raw %>%
#     slice(1:n_days) %>%
#     pull(href) %>%
#     unname()
#
#   return(tpr_urls)
# }
#
# # download new files --------------------------------------------------------------------------------------------
# N_TPR_DAYS = 1
# tpr_urls   = Get_TPR_Urls(n_days = N_TPR_DAYS)
# dl_results = Manage_TPR_Download(tpr_urls, n_days)
#
# if (length(dl_results$new_files) > 0 | N_TPR_DAYS > 1) {
#   tpr_file_paths_all = dl_results$diff$all_tpr_downloads
#   path_length        = ifelse(is.infinite(N_TPR_DAYS), length(tpr_file_paths_all), N_TPR_DAYS)
#   tpr_file_paths     = tpr_file_paths_all[(length(tpr_file_paths_all) - path_length + 1):length(tpr_file_paths_all)]
#
#   plan(multicore, workers = parallel::detectCores())
#   all_results      = furrr::future_map(tpr_file_paths, Clean_TPR, .progress = TRUE)
#   cpr_date_results = furrr::future_map(tpr_file_paths, Get_CPR_Date, .progress = TRUE)
#   plan(sequential)
#   split_tpr = Split_TPR(all_results, cpr_date_results)
#   Save_TPR_Results(split_tpr)
# }
#
# cpr_tpr_file_paths = list.files('original-sources/historical/cpr/', full.names = TRUE)
# tpr_results_cleaned        = map(cpr_tpr_file_paths, fread) %>%
#   rbindlist() %>%
#   mutate(Date = as.Date(Date)) %>%
#   distinct() %>%
#   group_by(Date, County) %>%
#   arrange(desc(file_path)) %>%
#   slice(1) %>%
#   ungroup()


Get_TPR_Data = function(county_name) {
  county_fips_lookup = fread('original-sources/helpers/county_fips_lookup.csv')
  tpr_url_base_path  = 'https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=integrated_county_timeseries_fips_'
  county_fips        = county_fips_lookup %>%
    filter(County == county_name) %>%
    pull(fips)
  data_raw           = read_json(glue('{tpr_url_base_path}{county_fips}_external'))

  return(data_raw)
}

Clean_TPR_Data = function(county_name) {
  data_cleaned = suppressWarnings(
  { tpr_results_raw[[county_name]][['integrated_county_timeseries_external_data']] %>%
    rbindlist(fill = TRUE) %>%
    mutate(County = county_name) %>%
    select(County, date, percent_positive_7_day, new_test_results_reported_7_day_rolling_average) %>%
    rename(
      Date  = date,
      TPR   = percent_positive_7_day,
      Tests = new_test_results_reported_7_day_rolling_average
    )
  }
  )
  return(data_cleaned)
}


# 23 seconds
start_time = Sys.time()
plan(multisession, workers = parallel::detectCores())
tpr_results_raw = furrr::future_map(
  county_names_vec,
  Get_TPR_Data,
  .progress = TRUE
)
plan(sequential)
print(Sys.time() - start_time)

names(tpr_results_raw) = county_names_vec

tpr_results_cleaned_raw = map(
  names(tpr_results_raw),
  ~Clean_TPR_Data(.)
)

tpr_results_cleaned = tpr_results_cleaned_raw %>%
  rbindlist() %>%
  arrange(County, Date) %>%
  mutate(TPR = TPR / 100) %>%
  mutate(Tests = as.integer(floor(Tests)))

arrow::write_parquet(tpr_results_cleaned, glue('original-sources/historical/cdc_tpr/{date_out}_cpr_tpr.parquet'))
# vitals --------------------------------------------------------------------------------------------
## globals --------------------------------------------------------------------------------------------
dshs_base_url                   = 'https://www.dshs.texas.gov/sites/default/files/chs/data/COVID'
confirmed_case_url              = glue("{dshs_base_url}/Texas%20COVID-19%20New%20Confirmed%20Cases%20by%20County.xlsx")
probable_case_url               = glue('{dshs_base_url}/Texas%20COVID-19%20New%20Probable%20Cases%20by%20County.xlsx')
PROBABLE_COMBINATION_START_DATE = as.Date('2022-04-01')

Clean_Cases = function(case_url) {
  temp = tempfile()
  curl::curl_download(case_url, temp, mode = 'wb')
  sheet_names = readxl::excel_sheets(temp)

  all_cases = map(sheet_names, ~readxl::read_xlsx(temp, sheet = ., col_types = 'text', skip = 2))

  cleaned_cases = all_cases %>%
    rbindlist(fill = TRUE) %>%
    pivot_longer(!County) %>%
    filter(!is.na(value)) %>%
    rename(Date        = name,
           Cases_Daily = value) %>%
    filter(!str_to_upper(Date) %in% c("TOTAL", "UNKNOWN DATE")) %>%
    mutate(Date = ifelse(!is.na(as.integer(Date)),
                         as.character(as.Date(as.integer(Date), origin = '1899-12-30')),
                         as.character(as.Date(Date, format = '%m/%d/%Y'))
    )
    ) %>%
    mutate(Date = as.Date(Date)) %>%
    mutate(Cases_Daily = as.integer(Cases_Daily)) %>%
    distinct()


  return(cleaned_cases)
}

confirmed_cases = Clean_Cases(confirmed_case_url)
probable_cases  = Clean_Cases(probable_case_url)

DSHS_cases_long = confirmed_cases %>%
  mutate(Case_Type = 'confirmed') %>%
  rbind(
    confirmed_cases %>%
      filter(Date >= PROBABLE_COMBINATION_START_DATE) %>%
      rbind(probable_cases %>% filter(Date >= PROBABLE_COMBINATION_START_DATE)) %>%
      mutate(Case_Type = 'confirmed_plus_probable')
  ) %>%
  mutate(Cases_Daily = ifelse(is.na(Cases_Daily), 0, Cases_Daily)) %>%
  group_by(County, Date, Case_Type) %>%
  summarize(Cases_Daily = sum(Cases_Daily, na.rm = TRUE))

max_case_date = max(DSHS_cases_long$Date, na.rm = TRUE)

## deaths --------------------------------------------------------------------------------------------
death_url = glue('{dshs_base_url}/Texas%20COVID-19%20Fatality%20Count%20Data%20by%20County.xlsx')

temp = tempfile()
curl::curl_download(death_url, temp, mode = 'wb')
sheet_names = readxl::excel_sheets(temp)

all_fatalities = map(sheet_names, ~readxl::read_xlsx(temp, sheet = ., col_types = 'text', skip = 2))

DSHS_deaths_long = all_fatalities %>%
  rbindlist(fill = TRUE) %>%
  pivot_longer(!County) %>%
  filter(!is.na(value)) %>%
  rename(Date              = name,
         Deaths_Cumulative = value) %>%
  filter(!str_to_upper(Date) %in% c("TOTAL", "UNKNOWN DATE")) %>%
  mutate(Date = ifelse(!is.na(as.integer(Date)),
                       as.character(as.Date(as.integer(Date), origin = '1899-12-30')),
                       as.character(as.Date(Date, format = '%m/%d/%Y'))
  )
  ) %>%
  mutate(Date = as.Date(Date)) %>%
  mutate(Deaths_Cumulative = as.integer(Deaths_Cumulative)) %>%
  group_by(County) %>%
  arrange(Date) %>%
  mutate(Deaths_Daily = Deaths_Cumulative - lag(Deaths_Cumulative)) %>%
  select(-Deaths_Cumulative) %>%
  ungroup() %>%
  mutate(Deaths_Daily = ifelse(is.na(Deaths_Daily), 0, Deaths_Daily)) %>%
  distinct()

max_death_date = max(DSHS_cases_long$Date, na.rm = TRUE)

## combine --------------------------------------------------------------------------------------------
DSHS_vitals_long = DSHS_cases_long %>%
  left_join(DSHS_deaths_long, by = c('County', 'Date')) %>%
  arrange(County, Date) %>%
  mutate(across(c(Cases_Daily, Deaths_Daily), ~ifelse(is.na(.) | . < 0, 0, .))) %>%
  group_by(County) %>%
  mutate(Cases_Cumulative = cumsum(Cases_Daily)) %>%
  mutate(Deaths_Cumulative = cumsum(Deaths_Daily)) %>%
  ungroup() %>%
  distinct() %>%
  filter(!is.na(Date))

stopifnot(
  DSHS_vitals_long %>%
    filter(Cases_Daily < 0) %>%
    nrow() == 0
)

stopifnot(DSHS_vitals_long %>%
            group_by(County, Date, Case_Type) %>%
            filter(n() > 1) %>%
            nrow()
            == 0
)
## merge data --------------------------------------------------------------------------------------------
county_tests = tpr_results_cleaned %>%
  select(County, Date, Tests) %>%
  rename(Tests_Daily = Tests) %>%
  filter(!is.na(Tests_Daily)) %>%
  group_by(County) %>%
  arrange(Date) %>%
  mutate(Tests_Cumulative = cumsum(Tests_Daily)) %>%
  filter(County %in% county_metadata$County) %>%
  mutate(Date = as.Date(Date))


merged_dshs = DSHS_vitals_long %>%
  filter(County %in% county_metadata$County) %>%
  left_join(county_populations, by = 'County') %>%
  left_join(county_tests, by = c('County', 'Date')) %>%
  left_join(county_metadata %>% select(County, TSA, TSA_Name, TSA_Combined, PHR, PHR_Name, PHR_Combined, Metro_Area), by = 'County') %>%
  filter(Date >= as.Date('2020-03-06') & !is.na(County)) %>%
  distinct() %>%
  mutate(Population_DSHS = case_when(
    Date < '2020-07-01' ~ Population_2020_04_01,
    Date >= '2020-07-01' & Date < '2021-07-01' ~ Population_2020_07_01,
    Date >= '2021-07-01' ~ Population_2021_07_01,
    TRUE ~ NA
  )) %>%
  select(-Population_2020_04_01, -Population_2020_07_01, -Population_2021_07_01) %>%
  select(Date, County, Case_Type,
         Cases_Cumulative, Cases_Daily,
         Deaths_Cumulative, Deaths_Daily,
         Tests_Cumulative, Tests_Daily,
         Population_DSHS,
         TSA, TSA_Name, TSA_Combined,
         PHR, PHR_Name, PHR_Combined, Metro_Area
  ) %>%
  arrange(County, Date, Case_Type)

# diagnostic --------------------------------------------------------------------------------------------
stopifnot(merged_dshs$County %>% unique() %>% length() == 254)
stopifnot(merged_dshs$TSA_Combined %>%
            unique() %>%
            length() == 22)
stopifnot(!is.na(merged_dshs$Date))
fwrite(merged_dshs %>% arrange(County, Date), 'tableau/county.csv')

# TPR --------------------------------------------------------------------------------------------
# tpr_df = rbindlist(
#   lapply(list.files('original-sources/historical/cms_tpr/', full.names = TRUE), read.csv),
#   fill = TRUE) %>%
#   mutate(Date = as.Date(Date)) %>%
#   rename(TPR = TPR_CMS) %>%
#   mutate(TPR = ifelse(Date >= '2020-12-16', NA, TPR))
#
# # add cms archive (8/19 - 12/09) (data represented 2 weeks of cases)
# cpr_dates = list.files('original-sources/historical/cpr') %>%
#   gsub('cpr_tpr_', '', .) %>%
#   gsub('.csv', '', .) %>%
#   as.Date(.)
#
# cms_dates = list.files('original-sources/historical/cms_tpr/') %>%
#   gsub('TPR_', '', .) %>%
#   gsub('.csv', '', .) %>%
#   as.Date(.)
#
# TPR_dates     = sort(unique(c(cpr_dates, cms_dates)))
# TPR_all_dates = data.frame(
#   County = rep(county_metadata$County, each = length(TPR_dates)),
#   Date   = rep(TPR_dates, times = length(county_metadata$County))
# )
#
# cms_archive = tpr_df %>%
#   filter(Date < '2020-12-16') %>%
#   select(Date, County, Tests, TPR)
#
# cms_new = tpr_df %>%
#   filter(Date >= '2020-12-16') %>%
#   rbind(TPR_all_dates %>%
#           filter(Date >= '2020-12-16'),
#         fill = TRUE) %>%
#   distinct() %>%
#   select(Date, County, Tests) %>%
#   filter(!is.na(Tests))

tpr_cases = merged_dshs %>%
  dplyr::select(County, Date, Cases_Daily, Population_DSHS) %>%
  # filter(Date >= as.Date(min(TPR_dates)) - 13 & Date <= max(TPR_dates)) %>%
  group_by(County) %>%
  mutate(Cases_100K_7Day_MA = (rollmean(Cases_Daily, k = 7, align = 'right',
                                        na.pad         = TRUE, na.rm = TRUE)
    / Population_DSHS) * 100000) %>%
  mutate(Cases_100K_14Day_MA = (rollmean(Cases_Daily, k = 14, align = 'right',
                                         na.rm          = TRUE, na.pad = TRUE)
    / Population_DSHS) * 100000) %>%
  # filter(Date %in% TPR_dates) %>%
  dplyr::select(-Cases_Daily, -Population_DSHS, -Cases_100K_14Day_MA)

tpr_out = tpr_results_cleaned %>%
  mutate(Date = as.Date(Date)) %>%
  # rbind(TPR_all_dates %>% filter(Date >= '2020-12-16'), fill = TRUE) %>%
  distinct() %>%
  arrange(County, Date) %>%
  # rbind(cms_archive) %>%
  # left_join(cms_new, by = c('County', 'Date')) %>%
  left_join(tpr_cases, by = c('County', 'Date')) %>%
  # mutate(Tests.x = ifelse(is.na(Tests.y), Tests.x, Tests.y)) %>%
  # rename(Tests = Tests.x) %>%
  # dplyr::select(-Tests.y) %>%
  dplyr::select(County, Date, TPR, Tests, Cases_100K_7Day_MA) %>%
  arrange(County, Date) %>%
  group_by(County, Date) %>%
  slice(1) %>%
  distinct() %>%
  group_by(Date) %>%
  mutate(count_0 = sum(TPR == 0)) %>%
  ungroup() %>%
  filter(count_0 < 254 | is.na(count_0))


fwrite(tpr_out, 'tableau/county_TPR.csv')

# vaccinations --------------------------------------------------------------------------------------------
Create_Agesex_Population_df = function(county_demo_agesex, pop_col) {
  pop_date_df = county_demo_agesex %>%
    filter(Age_Group %in% c('5+ years', '12+ years', '16+ years', '65+ years')) %>%
    select(all_of(c('County', 'Gender', 'Age_Group', pop_col))) %>%
    rename('Population_DSHS' = pop_col) %>%
    summarize(Population_DSHS = sum(Population_DSHS), .by = c(County, Age_Group)) %>%
    pivot_wider(names_from  = Age_Group,
                values_from = Population_DSHS
    ) %>%
    left_join(county_populations %>% select(all_of(c('County', pop_col))), by = 'County') %>%
    rename('Population_Total' = pop_col) %>%
    rename(Population_5 = `5+ years`, Population_12 = `12+ years`, Population_16 = `16+ years`, Population_65 = `65+ years`)
  return(pop_date_df)
}

county_vaccinations_out_pops = Create_Agesex_Population_df(county_demo_agesex, 'Population_2021_07_01')
county_vax_fix_2020_07_01    = Create_Agesex_Population_df(county_demo_agesex, 'Population_2020_07_01')

## download --------------------------------------------------------------------------------------------
vaccine_county_dshs_url_base = 'https://www.dshs.texas.gov/sites/default/files/LIDS-Immunize-COVID19/COVID%20Dashboard/County%20Dashboard/COVID-19%20Vaccine%20Data%20by%20County_'
if (format(date_out, '%A') == 'Wednesday') {
  vaccine_county_dshs_url = str_c(vaccine_county_dshs_url_base, format(date_out, '%Y%m%d'), '.xlsx')

  try(
    curl::curl_download(
      vaccine_county_dshs_url,
      mode     = 'wb',
      destfile = glue('original-sources/historical/vaccinations/vaccinations_{date_out}.xlsx')
    ),
    silent = TRUE
  )
}

## parse --------------------------------------------------------------------------------------------
dashboard_archive        = list.files('original-sources/historical/vaccinations', full.names = TRUE)
current_vaccination_file = fread('tableau/sandbox/county_daily_vaccine.csv')

NUM_DAYS                   = 1L
BIVALENT_START_DATE        = as.Date('2022-09-05')
new_files                  = dashboard_archive[(length(dashboard_archive) - (NUM_DAYS - 1)):length(dashboard_archive)]
all_dashboard_files        = lapply(new_files, read_excel_allsheets, add_date = TRUE, col_option = FALSE, skip_option = 0)
names(all_dashboard_files) = new_files


county_dashboard_data_exists      = map_lgl(all_dashboard_files, ~'By County' %in% names(.))
county_dashboard_files_nonmissing = all_dashboard_files[county_dashboard_data_exists]

vaccine_file_dates_raw = map(county_dashboard_files_nonmissing,
                             ~.[['About the Data']] %>%
                               slice(1) %>%
                               setNames(c('Colname', 'Date_raw')) %>%
                               select(Colname, Date_raw)
)

vaccine_file_dates = vaccine_file_dates_raw %>%
  rbindlist(fill = TRUE) %>%
  mutate(file_path = names(county_dashboard_files_nonmissing)) %>%
  mutate(Date_file = str_extract(file_path, '\\d{4}-\\d{2}-\\d{2}')) %>%
  mutate(Date_raw = ifelse(!str_detect(Date_raw, '\\d{5}'), NA, Date_raw)) %>%
  mutate(Date_raw_parsed = as.Date(as.integer(Date_raw), origin = '1899-12-30')) %>%
  mutate(Date = ifelse(is.na(Date_raw), as.character(Date_file), as.character(Date_raw_parsed))) %>%
  mutate(Date = as.Date(Date)) %>%
  select(file_path, Date) %>%
  arrange(desc(file_path)) %>%
  group_by(Date) %>%
  slice(1) %>%
  ungroup()

county_vaccinations_raw = map(
  names(county_dashboard_files_nonmissing),
  function(x) return(
    county_dashboard_files_nonmissing[[x]][['By County']] %>%
      setNames(slice(., 1)) %>%
      slice(2:nrow(.)) %>%
      mutate(file_path = x))
)

county_vaccinations_combined = county_vaccinations_raw %>%
  rbindlist(fill = TRUE) %>%
  left_join(vaccine_file_dates, by = 'file_path') %>%
  rename_with(~case_when(
    . == "County Name" ~ "County",
    . == "Vaccine Doses Administered" ~ "Doses_Administered",
    . == "People Vaccinated with at least One Dose" ~ "At_Least_One_Dose",
    . == "People Fully Vaccinated" ~ "Fully_Vaccinated",
    # . == "People Vaccinated with Booster Dose" ~ "Boosted",
    . == "People Vaccinated with at least One Booster Dose" ~ "Boosted",
    TRUE ~ .
  )) %>%
  filter(County %in% county_metadata$County) %>%
  # mutate(Boosted = ifelse(is.na(Boosted1), Boosted2, Boosted1)) %>%
  select(County, Date, Doses_Administered, At_Least_One_Dose, Fully_Vaccinated, Boosted) %>%
  mutate(across(c(Doses_Administered, At_Least_One_Dose, Fully_Vaccinated, Boosted), as.integer)) %>%
  distinct()

county_vaccinations_prefinal = county_vaccinations_combined %>%
  filter(Date < '2021-07-01') %>%
  left_join(county_vax_fix_2020_07_01, by = 'County') %>%
  rbind(
    county_vaccinations_combined %>%
      filter(Date >= '2021-07-01') %>%
      left_join(county_vaccinations_out_pops, by = 'County')
  ) %>%
  mutate(Date = as.Date(Date)) %>%
  left_join(county_metadata %>% select(County, TSA_Combined, PHR_Combined, Metro_Area), by = 'County') %>%
  select(Date, County, Doses_Administered, At_Least_One_Dose, Fully_Vaccinated, Boosted,
         Population_5, Population_12, Population_16, Population_65, Population_Total,
         TSA_Combined, PHR_Combined, Metro_Area
  )

county_vaccinations = county_vaccinations_prefinal %>%
  mutate(Vaccination_Type = 'all') %>%
  rbind(
    county_vaccinations_prefinal %>%
      filter(Date >= BIVALENT_START_DATE) %>%
      mutate(Vaccination_Type = 'bivalent') %>%
      group_by(County) %>%
      mutate(Boosted = Boosted - Boosted[1]) %>%
      mutate(Boosted = ifelse(Boosted < 0, 0L, Boosted)) %>%
      ungroup()
  ) %>%
  relocate(Vaccination_Type, .after = 'County') %>%
  rbind(current_vaccination_file %>% mutate(Date = as.Date(Date))) %>%
  distinct()

check_dupes = county_vaccinations %>%
  group_by(County, Date, Vaccination_Type) %>%
  summarise(n = n()) %>%
  filter(n > 1) %>%
  nrow() == 0

check_nonmissing_col = county_vaccinations %>%
  filter(if_any(c(County, TSA_Combined, PHR_Combined, Metro_Area), ~is.na(.))) %>%
  nrow() == 0

stopifnot(c(check_dupes, check_nonmissing_col))
fwrite(county_vaccinations, 'tableau/sandbox/county_daily_vaccine.csv')

# state  --------------------------------------------------------------------------------------------
dashboard_file_names = names(all_dashboard_files[[length(all_dashboard_files)]])
state_demo_file_name = dashboard_file_names[str_detect(dashboard_file_names, 'By Age, Gender|Sex, Race')]
state_demo_raw = all_dashboard_files[[length(all_dashboard_files)]][[state_demo_file_name]] %>%
  setNames(slice(., 1) %>% unlist()) %>%
  slice(2:nrow(.)) %>%
  select(-ncol(.)) %>%
  mutate(Date = max(vaccine_file_dates$Date)) %>%
  mutate(`Race/Ethnicity` = ifelse(is.na(`Race/Ethnicity`), str_c(Race, Ethnicity), `Race/Ethnicity`)) %>%
  select(-any_of(c('Race', 'Ethnicity', 'People Vaccinated with at Least One Dose', 'People Vaccinated'))) %>%
  unite(Age_Group, starts_with('Age'), remove = TRUE, na.rm = TRUE) %>%
  unite(Boosted, contains('Booster'), remove = TRUE, na.rm = TRUE) %>%
  rename(
    any_of(
      c(
        Doses_Administered      = 'Doses Administered',
        At_Least_One_Vaccinated = 'People Vaccinated with at least One Dose',
        Fully_Vaccinated        = 'People Fully Vaccinated',
        Fully_Vaccinated        = 'People Fully Vaccinated ',
        Gender                  = 'Sex'
      )
    )
  ) %>%
  filter(Age_Group != 'Total') %>%
  mutate(Age_Group = str_replace_all(Age_Group, 'yr', '')) %>%
  mutate(Age_Group = str_replace_all(Age_Group, 'mo', ' months')) %>%
  mutate(Age_Group = ifelse(Age_Group %in% c('44692', '45057'), '5-11 years', Age_Group)) %>%
  mutate(Age_Group = ifelse(Age_Group %in% c('45275', '44910'), '12-15 years', Age_Group)) %>%
  mutate(Age_Group = ifelse(!str_detect(Age_Group, 'years|Unknown'), glue('{Age_Group} years'), Age_Group)) %>%
  mutate(Age_Group = str_squish(Age_Group))


# add pops
state_demo_prep = state_demo_raw %>%
  left_join(county_demo_race %>%
              filter(Age_Group == 'total') %>%
              group_by(`Race/Ethnicity`) %>%
              summarize(State_Race_Total = sum(Population_Total, na.rm = TRUE)),
            by = 'Race/Ethnicity'
  ) %>%
  left_join(county_demo_agesex %>%
              filter(Age_Group == 'Total') %>%
              select(Gender, Population_2021_07_01) %>%
              group_by(Gender) %>%
              summarize(State_Gender_Total = sum(Population_2021_07_01, na.rm = TRUE)),
            by = 'Gender'
  ) %>%
  left_join(county_demo_agesex %>%
              group_by(Age_Group) %>%
              summarize(State_Age_Total = sum(Population_2021_07_01, na.rm = TRUE)),
            by = 'Age_Group'
  ) %>%
  mutate(across(-c(Gender, Age_Group, `Race/Ethnicity`), as.integer)) %>%
  mutate(Doses_Administered_Per_Race        = Doses_Administered / State_Race_Total,
         Doses_Administered_Per_Gender      = Doses_Administered / State_Gender_Total,
         Doses_Administered_Per_Age         = Doses_Administered / State_Age_Total,
         At_Least_One_Vaccinated_Per_Race   = At_Least_One_Vaccinated / State_Race_Total,
         At_Least_One_Vaccinated_Per_Gender = At_Least_One_Vaccinated / State_Gender_Total,
         At_Least_One_Vaccinated_Per_Age    = At_Least_One_Vaccinated / State_Age_Total,
         Fully_Vaccinated_Per_Race          = Fully_Vaccinated / State_Race_Total,
         Fully_Vaccinated_Per_Gender        = Fully_Vaccinated / State_Gender_Total,
         Fully_Vaccinated_Per_Age           = Fully_Vaccinated / State_Age_Total,
         Boosted_Per_Race                   = Boosted / State_Race_Total,
         Boosted_Per_Gender                 = Boosted / State_Gender_Total,
         Boosted_Per_Age                    = Boosted / State_Age_Total) %>%
  relocate(`Race/Ethnicity`, .after = Age_Group) %>%
  relocate(Date, .after = Boosted_Per_Age) %>%
  mutate(Date = format(as.Date(Date, origin = '1970-01-01'), '%Y-%m-%d'))

check_state_demo_dupes = state_demo_prep %>%
  group_by(Gender, Age_Group, `Race/Ethnicity`) %>%
  filter(n() > 1) %>%
  nrow() == 0

stopifnot(check_state_demo_dupes)

# add bivalent group
bivalent_baseline = fread('tableau/helpers/state_demo_bivalent_baseline.csv')
state_demo        = state_demo_prep %>%
  mutate(Vaccination_Type = 'all') %>%
  rbind(
    state_demo_prep %>%
      mutate(Vaccination_Type = 'bivalent') %>%
      left_join(bivalent_baseline %>%
                  select(-Date) %>%
                  rename(Boosted_baseline = Boosted),
                by = c('Gender', 'Age_Group', 'Race/Ethnicity')
      ) %>%
      mutate(Boosted = Boosted - Boosted_baseline) %>%
      mutate(Boosted_Per_Race   = Boosted / State_Race_Total,
             Boosted_Per_Gender = Boosted / State_Gender_Total,
             Boosted_Per_Age    = Boosted / State_Age_Total) %>%
      select(-Boosted_baseline) %>%
      mutate(Date = max(state_demo_prep$Date))
  ) %>%
  relocate(Vaccination_Type, .after = `Race/Ethnicity`) %>%
  relocate(Date, .before = 'Gender')

fwrite(state_demo, 'tableau/sandbox/state_vaccine_demographics.csv')
#  --------------------------------------------------------------------------------------------
measure_cols             = c('Doses_Administered', 'At_Least_One_Vaccinated', 'Fully_Vaccinated', 'Boosted')
state_demo_stacked_clean = state_demo %>%
  #-------------------------------------------Gender------------------------------------
  select(all_of(c('Gender', 'Vaccination_Type', measure_cols))) %>%
  mutate(Group_Type = 'Gender') %>%
  rename(Group = Gender) %>%
  summarize(across(all_of(measure_cols), ~sum(., na.rm = TRUE)), .by = c(Group_Type, Group, Vaccination_Type)) %>%
  # ----------------------------------------Race-----------------------------------------
  rbind(
    state_demo %>%
      select(all_of(c('Race/Ethnicity', 'Vaccination_Type', measure_cols))) %>%
      mutate(Group_Type = 'Race') %>%
      rename(Group = `Race/Ethnicity`) %>%
      summarize(across(all_of(measure_cols), ~sum(., na.rm = TRUE)), .by = c(Group_Type, Group, Vaccination_Type))
  ) %>%
  #  --------------------------------------Age------------------------------------------------------
  rbind(
    state_demo %>%
      select(all_of(c('Age_Group', 'Vaccination_Type', measure_cols))) %>%
      mutate(Group_Type = 'Age') %>%
      rename(Group = Age_Group) %>%
      summarize(across(all_of(measure_cols), ~sum(., na.rm = TRUE)), .by = c(Group_Type, Group, Vaccination_Type))
  ) %>%
  relocate(Group_Type, .before = Group) %>%
  left_join(state_demo_pops, by = c('Group_Type', 'Group')) %>%
  mutate(across(measure_cols, ~ifelse(. < 0, 0L, .)))

fwrite(state_demo_stacked_clean, 'tableau/sandbox/stacked_state_vaccine_demographics.csv')

#  --------------------------------------------------------------------------------------------
county_vax_race_prep = lapply(all_dashboard_files, `[[`, 'By County, Race') %>%
  discard(is.null) %>%
  .[[length(.)]] %>%
  setNames(slice(., 1)) %>%
  slice(2:nrow(.)) %>%
  select(1:(ncol(.) - 1)) %>%
  setNames(c('County', 'Race/Ethnicity',
             'At_Least_One_Vaccinated', 'Fully_Vaccinated', 'Boosted', 'Doses_Administered')) %>%
  left_join(county_demo_race %>%
              filter(Age_Group == 'total') %>%
              select(County, `Race/Ethnicity`, Population_Total),
            by = c('County', 'Race/Ethnicity')
  ) %>%
  mutate(across(-c(County, `Race/Ethnicity`), as.integer)) %>%
  filter(!(County %in% c('Other', 'Grand Total')))

county_race_bivalent_baseline = fread('tableau/helpers/county_bivalent_baseline_race.csv')

county_vax_race = county_vax_race_prep %>%
  mutate(Vaccination_Type = 'all') %>%
  rbind(
    county_vax_race_prep %>%
      left_join(county_race_bivalent_baseline %>%
                  select(-Date) %>%
                  rename(Boosted_baseline = Boosted),
                by = c('County', 'Race/Ethnicity')
      ) %>%
      mutate(Boosted = Boosted - Boosted_baseline) %>%
      select(-Boosted_baseline) %>%
      mutate(Vaccination_Type = 'bivalent')
  ) %>%
  mutate(across(-c(County, `Race/Ethnicity`, Vaccination_Type), ~ifelse(. < 0, 0L, .))) %>%
  relocate(Vaccination_Type, .after = `Race/Ethnicity`)

fwrite(county_vax_race, 'tableau/sandbox/county_vax_race.csv')
#  --------------------------------------------------------------------------------------------

county_vax_age_prep = lapply(all_dashboard_files, `[[`, 'By County, Age') %>%
  discard(is.null) %>%
  .[[length(.)]] %>%
  setNames(slice(., 1)) %>%
  slice(2:nrow(.)) %>%
  select(1:(ncol(.) - 1)) %>%
  rename(any_of(c(Age_Group = 'Agegrp',
                  Age_Group = 'Agegrp (group) 1',
                  Age_Group = 'Agegrp_v2'
  ))
  ) %>%
  mutate(Age_Group = ifelse(Age_Group %in% c('44692', '45057'), '5-11 years', Age_Group)) %>%
  mutate(Age_Group = ifelse(Age_Group %in% c('45275', '44910'), '12-15 years', Age_Group)) %>%
  setNames(c('County', 'Age_Group',
             'Doses_Administered', 'At_Least_One_Vaccinated', 'Fully_Vaccinated', 'Boosted')) %>%
  mutate(Age_Group = glue('{ Age_Group } years')) %>%
  mutate(Age_Group = gsub('years years', 'years', Age_Group)) %>%
  mutate(Age_Group = gsub('Unknown years', 'Unknown', Age_Group)) %>%
  mutate(Age_Group = str_replace_all(Age_Group, '6mo-4yr years', '6 months-4 years')) %>%
  mutate(Age_Group = str_squish(Age_Group)) %>%
  left_join(county_demo_agesex %>%
              select(County, Age_Group, Population_2021_07_01) %>%
              rename(Population_Total = Population_2021_07_01) %>%
              group_by(County, Age_Group) %>%
              summarize(Population_Total = sum(Population_Total, na.rm = TRUE)),
            by = c('County', 'Age_Group')
  ) %>%
  mutate(across(-c(County, Age_Group), as.integer)) %>%
  filter(!(County %in% c('Other', 'Grand Total')))

county_age_bivalent_baseline = fread('tableau/helpers/county_bivalent_baseline_age.csv')

county_vax_age = county_vax_age_prep %>%
  mutate(Vaccination_Type = 'all') %>%
  rbind(
    county_vax_age_prep %>%
      left_join(county_age_bivalent_baseline %>%
                  select(-Date) %>%
                  rename(Boosted_baseline = Boosted),
                by = c('County', 'Age_Group')
      ) %>%
      mutate(Boosted = Boosted - Boosted_baseline) %>%
      select(-Boosted_baseline) %>%
      mutate(Vaccination_Type = 'bivalent')
  ) %>%
  mutate(across(-c(County, Age_Group, Vaccination_Type), ~ifelse(. < 0, 0L, .))) %>%
  relocate(Vaccination_Type, .after = Age_Group)

fwrite(county_vax_age, 'tableau/sandbox/county_vax_age.csv')
# TSA --------------------------------------------------------------------------------------------
DSHS_tsa_counts =
  merged_dshs %>%
    group_by(Date, TSA, TSA_Name) %>%
    summarize(
      across(c(Cases_Cumulative, Cases_Daily,
               Deaths_Cumulative, Deaths_Daily),
             ~sum(., na.rm = TRUE))
    )

# static pop counts (sum)
DSHS_tsa_pops = merged_dshs %>%
  filter(Date == '2020-03-06') %>%
  group_by(TSA) %>%
  summarize(across(Population_DSHS, ~sum(., na.rm = TRUE)))


DSHS_tsa   = DSHS_tsa_counts %>%
  left_join(DSHS_tsa_pops, by = 'TSA')
# hospitals --------------------------------------------------------------------------------------------
## TSA excel sheet --------------------------------------------------------------------------------------------
TSA_source = 'https://www.dshs.texas.gov/sites/default/files/chs/data/COVID/Combined%20Hospital%20Data%20over%20Time%20by%20TSA%20Region.xlsx'
curl::curl_download(TSA_source, glue('original-sources/historical/hosp_xlsx/hosp_{date_out}.xlsx'))
TSA_hosp_all_sheets = read_excel_allsheets(glue('original-sources/historical/hosp_xlsx/hosp_{date_out}.xlsx'), skip_option = 2)

names(TSA_hosp_all_sheets)


TSA_hosp_combined = map(
  names(TSA_hosp_all_sheets),
  function(x) {
    message(x)

    raw_sheet = TSA_hosp_all_sheets[[x]]
    if (!any(str_detect(names(raw_sheet), 'TSA'))) {
      col_row   = which(str_detect(raw_sheet[, 1], 'TSA'))
      raw_sheet = raw_sheet %>%
        setNames(raw_sheet[col_row,]) %>%
        slice((col_row + 1):nrow(raw_sheet))

    }

    cleaned_sheet = raw_sheet %>%
      select(-any_of(c("TSA.AREA"))) %>%
      rename(any_of(c(TSA = 'TSA ID', TSA = 'TSA.ID'))) %>%
      pivot_longer(!TSA) %>%
      mutate(Hosp_Var = !!quo_name(x)) %>%
      rename(Date = name) %>%
      relocate(Hosp_Var, .before = 'value')
    return(cleaned_sheet)
  }
) %>%
  rbindlist(fill = TRUE)

TSA_hosp_combined_cleaned = TSA_hosp_combined %>%
  mutate(TSA = str_replace_all(TSA, '\\.', '')) %>%
  filter(str_length(TSA) == 1) %>%
  filter(!is.na(value)) %>%
  filter(!str_detect(Hosp_Var, '%')) %>%
  mutate(value = as.integer(value)) %>%
  pivot_wider(id_cols     = c(Date, TSA),
              names_from  = Hosp_Var,
              values_from = value) %>%
  rename(
    all_of(
      c(
        # ICU----------------------------------------------------
        "Beds_Occupied_ICU"            = "ICU Beds Occupied",
        "Beds_Available_ICU"           = "Adult ICU Beds Available",
        "Hospitalizations_ICU"         = "Adult COVID-19 ICU",

        # General------------------------------------------------
        "Hospitalizations_General"     = "Adult COVID-19 General",
        "Beds_Available_Total"         = "Total Available Beds",
        "Beds_Occupied_Total"          = "Total Occupied Beds",

        # Pediatric ---------------------------------------------
        "Hospitalizations_Pediatric"   = "Pediatric COVID-19",
        "Pediatric_Beds_Available_ICU" = "Pediatric ICU Beds Available",

        # Other
        "Hospitalizations_24"          = "COVID-19 Admits 24HR",
        "Ventilators_Available"        = "Available Ventilators"
      ))) %>%
  group_by(Date, TSA) %>%
  mutate(Hospitalizations_Total = sum(Hospitalizations_General, Hospitalizations_ICU, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(Date = as.Date(Date, '%m/%d/%Y')) %>%
  select(
    Date, TSA,
    Hospitalizations_Total, Hospitalizations_General, Hospitalizations_ICU,
    Ventilators_Available,
    Hospitalizations_Pediatric,
    Beds_Available_Total, Beds_Available_ICU, Beds_Occupied_Total,
    Beds_Occupied_ICU,
    Pediatric_Beds_Available_ICU, Hospitalizations_24
  ) %>%
  distinct() %>%
  group_by(TSA, Date) %>%
  arrange(Hospitalizations_Total) %>%
  slice(1) %>%
  ungroup()

stopifnot(TSA_hosp_combined_cleaned %>%
            group_by(TSA, Date) %>%
            filter(n() > 1) %>%
            nrow() == 0
)
# combine --------------------------------------------------------------------------------------------
merged_tsa = DSHS_tsa %>%
  left_join(TSA_hosp_combined_cleaned, by = c('TSA', 'Date')) %>%
  mutate(TSA_Combined = paste0(TSA, ' - ', TSA_Name)) %>%
  filter(!is.na(TSA) & !is.na(Date)) %>%
  distinct() %>%
  select(Date, TSA, TSA_Name, TSA_Combined, Population_DSHS,
         Hospitalizations_Total:Hospitalizations_24) %>%
  filter(Date >= as.Date('2020-04-11')) %>%
  arrange(TSA, Date) %>%
  distinct()

stopifnot(merged_tsa %>%
            group_by(Date, TSA) %>%
            filter(n() > 1) %>%
            nrow() == 0)


fwrite(merged_tsa, file = 'tableau/hospitalizations_tsa.csv')

# state level --------------------------------------------------------------------------------------------
# cdc variants --------------------------------------------------------------------------------------------
variant_lookup = data.frame(
  stringsAsFactors = FALSE,
  Variant          = c("B.1.1.7", "B.1.351", "P.1",
                       "B.1.617.2", "AY.1", "AY.2", "B.1.526", "B.1.617.1",
                       "B.1.621", "Other", "B.1.427/429", "B.1.525", "B.1.617.3",
                       "P.2"),
  Variant_Label    = c("Alpha", "Beta", "Gamma",
                       "Delta", "Delta", "Delta", "Iota", "Kappa", "Mu", "Other",
                       "Epsilon", "Eta", NA, "Zeta"))


cdc_variant_data = lapply(list.files('original-sources/historical/cdc_variants/', full.names = TRUE),
                          fread) %>%
  rbindlist(fill = TRUE) %>%
  group_by(Date) %>%
  slice(1) %>%
  ungroup() %>%
  unite(Sequences, c('Total Available sequences', 'Total Available Sequences', 'Total Sequences'), na.rm = TRUE) %>%
  select(-V3) %>%
  mutate(across(everything(), ~gsub('\\%|\\, ', '', .))) %>%
  mutate(across(-c(Date, State), as.numeric)) %>%
  reshape2::melt(id.vars = c('Date', 'State', 'Sequences')) %>%
  setNames(c('Date', 'State', 'Total_Sequences', 'Variant', 'Proportion')) %>%
  group_by(Date) %>%
  mutate(Proportion_Total = sum(Proportion, na.rm = TRUE)) %>%
  group_by(row_number()) %>%
  mutate(Proportion = ifelse(Proportion_Total > 1, Proportion / 100, Proportion)) %>%
  group_by(Date) %>%
  mutate(Proportion_Total1 = sum(Proportion, na.rm = TRUE)) %>%
  select(1:5) %>%
  ungroup() %>%
  mutate_if(is.factor, as.character) %>%
  left_join(variant_lookup, by = 'Variant') %>%
  group_by(Date, Variant_Label) %>%
  mutate(Proportion = sum(Proportion, na.rm = TRUE)) %>%
  filter(!Variant %in% c('AY.1', 'AY.2', 'B.1.617.3')) %>%
  select(Date, Variant, Variant_Label, Total_Sequences, Proportion) %>%
  mutate(Sequences = floor(Total_Sequences * Proportion)) %>%
  ungroup()

fwrite(cdc_variant_data, 'tableau/cdc_variant_data.csv')

# dshs demographics new --------------------------------------------------------------------------------------------
Clean_Demo_New = function(sheet_name) {
  message(sheet_name)
  if (str_detect(sheet_name, 'Fatal|Confirmed')) {

    group_type = str_extract(sheet_name, 'Age|Sex|Gender|Race')
    stat_type  = str_extract(sheet_name, 'Cases|Fatalities')

    df       = demographics_all[[sheet_name]]
    clean_df = df %>%
      filter(!is.na(`Month.Year`) & `Month.Year` != 'Grand Total') %>%
      mutate(across(everything(), as.character)) %>%
      pivot_longer(!1) %>%
      setNames(c('Date', 'Group', stat_type)) %>%
      filter(!str_detect(Date, 'Total|Notes')) %>%
      mutate(Date = as.Date(glue('01 {Date}'), '%d %B %Y')) %>%
      mutate(Group_Type = group_type) %>%
      mutate(across(-c(Date, Group_Type, Group), as.integer)) %>%
      filter(Group != 'Total') %>%
      filter(!is.na(Date))

    return(clean_df)
  }
}

# monthly
demo_releases         = seq(as.Date('2022-01-01'), by = 'month', length = 50) + days(14)
all_demo_files        = list.files('original-sources/historical/demo-archive', full.names = TRUE)
demo_update_threshold = 30L

last_monthly_demo_update = all_demo_files[[length(all_demo_files)]] %>%
  str_extract('\\d{4}-\\d{2}-\\d{2}') %>%
  as.Date()

last_update_delta = date_out - last_monthly_demo_update

if (date_out %in% demo_releases | last_update_delta > demo_update_threshold) {
  demo_url           = 'https://www.dshs.texas.gov/sites/default/files/chs/data/COVID/Texas%20COVID-19%20Demographics_Counts.xlsx'
  demo_file_path_new = glue('original-sources/historical/demo-archive/demo_{date_out}.xlsx')
  curl::curl_download(demo_url, demo_file_path_new, mode = 'wb')

  demographics_all = read_excel_allsheets(demo_file_path_new, skip = 3)

  actual_update_date_check = demographics_all[['Confirmed Cases by Age']] %>%
    filter(str_detect(Month.Year, as.character(year(date_out)))) %>%
    filter(str_detect(Month.Year, format(date_out - months(1), '%B')))

  if (nrow(actual_update_date_check) == 0) {
    file.remove(glue('original-sources/historical/demo-archive/demo_{date_out}.xlsx'))
    stop(
      str_c(
        'Case & Fatality demographics date does not match expected date.',
        ' Check https://www.dshs.texas.gov/covid-19-coronavirus-disease-2019/texas-covid-19-data to verify'
      )
    )
  }

  cleaned_demo_new = lapply(names(demographics_all), function(x) Clean_Demo_New(x)) %>%
    rbindlist(fill = TRUE) %>%
    rename(Deaths_Monthly = Fatalities,
           Cases_Monthly  = Cases) %>%
    mutate(across(c(Deaths_Monthly, Cases_Monthly), as.numeric)) %>%
    group_by(Date, Group_Type, Group) %>%
    tidyr::fill(Deaths_Monthly, .direction = 'updown') %>%
    tidyr::fill(Cases_Monthly, .direction = 'updown') %>%
    slice(1) %>%
    ungroup() %>%
    arrange(Date, Group_Type, Group) %>%
    relocate(Group_Type, .before = 'Group') %>%
    mutate(Group_Type = str_replace_all(Group_Type, c('Sex' = 'Gender'))) %>%
    mutate(Group = ifelse(Group_Type == 'Age' & !str_detect(Group, 'Unknown'), glue('{Group} years'), Group)) %>%
    group_by(Date, Group_Type) %>%
    mutate(Deaths_PCT = Deaths_Monthly / sum(Deaths_Monthly, na.rm = TRUE)) %>%
    mutate(Cases_PCT = Cases_Monthly / sum(Cases_Monthly, na.rm = TRUE)) %>%
    filter(!is.na(Date))
  # mutate(Date = ifelse(is.na(Date), 'Unknown', as.character(Date)))
  fwrite(cleaned_demo_new, 'tableau/stacked_demographics_v2.csv')
}
