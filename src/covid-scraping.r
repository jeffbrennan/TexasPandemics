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
# add metro and PHR code designations
# source: https://www.dshs.state.tx.us/chs/info/TxCoPhrMsa.xls
# add PHR readable names from https://dshs.state.tx.us/regions/default.shtm
PHR_helper = data.frame(PHR      = c("1", "2/3", "4/5N",
                                     "6/5S", "7", "8",
                                     "9/10", "11"),
                        PHR_Name = c('Lubbock PHR', 'Arlington PHR', 'Tyler PHR',
                                     'Houston PHR', 'Temple PHR', 'San Antonio PHR',
                                     'El Paso PHR', 'Harlingen PHR'))


county_classifications = read_xlsx('original-sources/helpers/county_classifications.xlsx', sheet = 1) %>%
  slice(1:254) %>%
  dplyr::select(1, 5, 8) %>%
  setNames(c('County', 'PHR', 'Metro_Area')) %>%
  left_join(., PHR_helper, by = 'PHR') %>%
  mutate(PHR_Combined = paste0(PHR, ' - ', PHR_Name))

tsa_lookup_url    = 'https://raw.githubusercontent.com/jeffbrennan/TexasPandemics/master/tableau/county.csv'
tsa_long_complete = fread(tsa_lookup_url, fill = TRUE) %>%
  select(County, TSA, TSA_Name, TSA_Combined) %>%
  distinct()

dshs_pops =
  fread('https://raw.githubusercontent.com/jeffbrennan/COVID-19/d03d476f7fb060dfd2e1a600a6a1e449df0ab8df/original-sources/DSHS_county_cases.csv') %>%
    select(County, Population) %>%
    distinct() %>%
    rename(Population_DSHS = Population)


## -county demographics --------------------------------------------------------------------------------------------
# https://www.census.gov/data/datasets/time-series/demo/popest/2010s-counties-detail.html
# 2019 county level race estimates
# collapse into asian, black, hispanic, white, other
# exclude totals to avoid double counting
race_group_df = data.frame(race       =
                             c('AA', 'AAC', 'BA', 'BAC', 'H', 'HAA',
                               'HAAC', 'HBA', 'HBAC', 'HIA', 'HIAC', 'HNA',
                               'HNAC', 'HTOM', 'HWA', 'HWAC', 'IA', 'IAC',
                               'NA', 'NAC', 'NH', 'NHAA', 'NHAAC', 'NHBA', 'NHBAC',
                               'NHIA', 'NHIAC', 'NHNA', 'NHNAC', 'NHTOM', 'NHWA',
                               'NHWAC', 'TOM', 'WA', 'WAC'),
                           race_group =
                             c(NA, NA, NA, NA, 'Hispanic', NA,
                               NA, NA, NA, NA, NA, NA,
                               NA, NA, NA, NA, NA, 'Other',
                               NA, 'Other', NA, 'Asian', NA, 'Black', NA,
                               NA, NA, NA, NA, NA, 'White',
                               NA, 'Other', NA, NA))


Get_County_Race = function(age_groups) {
  county_demo_race_prelim = fread('original-sources/helpers/demographics/county/county_pop_race.csv') %>%
    filter(YEAR == '12' & AGEGRP %in% age_groups) %>%
    select(-c(SUMLEV, STATE, COUNTY, STNAME, YEAR, AGEGRP, TOT_POP, TOT_MALE, TOT_FEMALE)) %>%
    reshape2::melt(idvars = 'CTYNAME') %>%
    separate(variable, c('race', 'gender')) %>%
    group_by(CTYNAME, race) %>%
    summarize(Total = sum(value, na.rm = TRUE)) %>%
    left_join(race_group_df) %>%
    group_by(CTYNAME, race_group) %>%
    summarize(Total = sum(Total, na.rm = TRUE)) %>%
    filter(!is.na(race_group)) %>%
    ungroup() %>%
    mutate(CTYNAME = gsub(' County', '', CTYNAME)) %>%
    setNames(c('County', 'Race/Ethnicity', 'Population_Total'))

  county_demo_other_race = fread('original-sources/helpers/demographics/county/county_pop_race.csv') %>%
    filter(YEAR == '12' & AGEGRP %in% age_groups) %>%
    select(-c(SUMLEV, STATE, COUNTY, STNAME, YEAR, AGEGRP, TOT_POP, TOT_MALE, TOT_FEMALE)) %>%
    reshape2::melt(idvars = 'CTYNAME') %>%
    separate(variable, c('race', 'gender')) %>%
    filter(race %in% c('AA', 'AAC', 'BA', 'BAC', 'NA', 'NAC', 'IA', 'IAC', 'WA', 'WAC')) %>%
    group_by(CTYNAME, race) %>%
    summarize(Total = sum(value, na.rm = TRUE)) %>%
    mutate(Total_combo = Total - lag(Total)) %>%
    filter(str_detect(race, 'C')) %>%
    mutate(`Race/Ethnicity` = 'Other') %>%
    mutate(CTYNAME = gsub(' County', '', CTYNAME)) %>%
    select(CTYNAME, `Race/Ethnicity`, Total_combo) %>%
    setNames(c('County', 'Race/Ethnicity', 'Population_Total'))

  county_demo_race = rbind(county_demo_race_prelim, county_demo_other_race) %>%
    group_by(County, `Race/Ethnicity`) %>%
    summarize(Population_Total = sum(Population_Total)) %>%
    ungroup() %>%
    arrange(County, `Race/Ethnicity`)

  return(county_demo_race)
}

county_demo_race        = Get_County_Race(c('0'))
county_demo_race_age_15 = Get_County_Race(as.character(seq(4, 18))) %>% rename('Population_16' = 'Population_Total')
county_demo_race_age_5  = Get_County_Race(as.character(seq(2, 18))) %>% rename('Population_5' = 'Population_Total')

## age+sex --------------------------------------------------------------------------------------------
age_lookup = data.frame(age       =
                          c('POPEST', 'UNDER5', 'AGE513', 'AGE1417', 'AGE1824',
                            'AGE16PLUS', 'AGE18PLUS', 'AGE1544', 'AGE2544',
                            'AGE4564', 'AGE65PLUS', 'AGE04', 'AGE59', 'AGE1014',
                            'AGE1519', 'AGE2024', 'AGE2529', 'AGE3034', 'AGE3539',
                            'AGE4044', 'AGE4549', 'AGE5054', 'AGE5559', 'AGE6064',
                            'AGE6569', 'AGE7074', 'AGE7579', 'AGE8084', 'AGE85PLUS'),
                        age_group =
                          c('Total', NA, NA, NA, NA,
                            '16+', NA, NA, NA,
                            NA, NA, NA, NA, NA,
                            NA, NA, NA, NA, NA,
                            NA, NA, '50-64 years', '50-64 years', '50-64 years',
                            '65-79 years', '65-79 years', '65-79 years', '80+ years', '80+ years'))

demo_12_15 = fread('original-sources/helpers/demographics/county/county_pop_age_sex.csv') %>%
  filter(YEAR == '12') %>%
  select(-c(SUMLEV, STATE, COUNTY, STNAME, YEAR,
            POPESTIMATE,
            MEDIAN_AGE_TOT, MEDIAN_AGE_MALE, MEDIAN_AGE_FEM)) %>%
  reshape2::melt(idvars = 'CTYNAME') %>%
  separate(variable, c('age', 'gender')) %>%
  filter(gender != 'TOT') %>%
  filter(age %in% c('AGE1014', 'AGE1519')) %>%
  mutate(value = ifelse(age == 'AGE1014', value * 0.6, value * 0.2)) %>%
  group_by(CTYNAME, gender) %>%
  summarize(Population_Total = round(sum(value), 0)) %>%
  mutate(`Age Group` = '12-15 years') %>%
  rename(County = CTYNAME, Gender = gender)

demo_5_11 = fread('original-sources/helpers/demographics/county/county_pop_age_sex.csv') %>%
  filter(YEAR == '12') %>%
  select(-c(SUMLEV, STATE, COUNTY, STNAME, YEAR,
            POPESTIMATE,
            MEDIAN_AGE_TOT, MEDIAN_AGE_MALE, MEDIAN_AGE_FEM)) %>%
  reshape2::melt(idvars = 'CTYNAME') %>%
  separate(variable, c('age', 'gender')) %>%
  filter(gender != 'TOT') %>%
  filter(age == 'AGE513') %>%
  mutate(value = ifelse(age == 'AGE513', value * (7 / 9), value)) %>%
  group_by(CTYNAME, gender) %>%
  summarize(Population_Total = round(sum(value), 0)) %>%
  mutate(`Age Group` = '5-11 years') %>%
  rename(County = CTYNAME, Gender = gender)

county_demo_agesex = fread('original-sources/helpers/demographics/county/county_pop_age_sex.csv') %>%
  filter(YEAR == '12') %>%
  select(-c(SUMLEV, STATE, COUNTY, STNAME, YEAR,
            POPESTIMATE,
            MEDIAN_AGE_TOT, MEDIAN_AGE_MALE, MEDIAN_AGE_FEM)) %>%
  reshape2::melt(idvars = 'CTYNAME') %>%
  separate(variable, c('age', 'gender')) %>%
  filter(gender != 'TOT') %>%
  left_join(age_lookup) %>%
  group_by(CTYNAME, age_group, gender) %>%
  summarize(Total = sum(value, na.rm = TRUE)) %>%
  filter(!is.na(age_group)) %>%
  spread(age_group, Total) %>%
  mutate('<16' = Total - `16+`) %>%
  mutate(`16-49 years` = `16+` -
    `50-64 years` -
    `65-79 years` -
    `80+ years`) %>%
  select(-Total) %>%
  reshape2::melt(idvars = c('CTYNAME', 'gender')) %>%
  setNames(c('County', 'Gender', 'Age Group', 'Population_Total')) %>%
  rbind(demo_12_15) %>%
  rbind(demo_5_11) %>%
  mutate(County = gsub(' County', '', County)) %>%
  mutate(Gender = recode(Gender, 'FEM' = 'Female', 'MALE' = 'Male'))

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
image_paths = c('omicron' = 'https://covidwwtp.spatialstudieslab.org/hhd/datasets/variants/B11529.png',
                'lambda'  = 'https://covidwwtp.spatialstudieslab.org/hhd/datasets/variants/C37.png',
                'delta'   = 'https://covidwwtp.spatialstudieslab.org/hhd/datasets/variants/B16172.png',
                'alpha'   = 'https://covidwwtp.spatialstudieslab.org/hhd/datasets/variants/B117.png')

walk(
  names(image_paths),
  ~download.file(image_paths[[.]],
                 glue('original-sources/historical/wastewater-variants/{.}_{date_out}.png'),
                 mode = 'wb')
)

# School level --------------------------------------------------------------------------------------------
nyt_schools = list.files('original-sources/historical/nyt/archive', full.names = TRUE) %>%
  map(., read_csv) %>%
  rbindlist() %>%
  # rbindlist(lapply(list.files('original-sources/historical/nyt/archive', full.names = TRUE), read.csv)) %>%
  setNames(c('School', 'City', 'County', 'Deaths_Cumulative', 'Cases_Cumulative', 'Date')) %>%
  mutate(Date = as.Date(Date)) %>%
  mutate(Cases_Cumulative = ifelse(Cases_Cumulative == -1, NA, Cases_Cumulative))

fwrite(nyt_schools, file = 'original-sources/historical/nyt/nyt_colleges.csv')

# # Retains only district level data (filters for rows containing total)
Clean_DSHS_Schools = function(df) {
  df_out = df %>%
    select(1:11) %>%
    setNames(c('District', 'LEA', 'District_Total_Enrollment',
               'Campus', 'Campus_ID', 'School_Total_Enrollment',
               'Cases_Student_New', 'Cases_Staff_New',
               'Case_Source_Campus', 'Case_Source_OffCampus', 'Case_Source_Unknown')) %>%
    select(-contains('School')) %>%
    filter(str_detect(District, 'TOTAL')) %>%
    mutate(District = str_squish(District)) %>%
    mutate(District = gsub(' TOTAL', '', District)) %>%
    mutate(Date = school_date) %>%
    relocate(Date, .after = District) %>%
    mutate(across(matches('Case'), as.numeric)) %>%
    mutate(across(c(LEA, Campus_ID), ~gsub("'", '', .)))
}

current_school_dates = seq(as.Date('2021-08-15'), by = 'week', length = 200)
school_date          = max(current_school_dates[which(current_school_dates <= date_out - 5)])
base_url             = 'https://dshs.state.tx.us/chs/data/tea/district-level-school-covid-19-case-data/campus-level-data_'

DSHS_schools =
  try(
    glue('{base_url}{format(school_date+2, "%Y%m%d")}v1.xls') %>%
      Download_Temp(.) %>%
      read_excel(sheet = 1) %>%
      data.frame() %>%
      Clean_DSHS_Schools(.)
  )

if (class(DSHS_schools) != 'try-error') {
  fwrite(DSHS_schools,
         file = glue('./original-sources/historical/dshs-schools/2021/{school_date}_DSHS_schools.csv'))
}


DSHS_schools_2021 = list.files('./original-sources/historical/dshs-schools/2021/', full.names = TRUE) %>%
  lapply(., fread) %>%
  rbindlist(fill = TRUE)

DSHS_schools_archive = fread('./original-sources/historical/dshs-schools/historical_archive.csv')

DSHS_schools_combined = DSHS_schools_2021 %>%
  plyr::rbind.fill(DSHS_schools_archive) %>%
  arrange(District, Date)


write_xlsx(
  list('school_data' = DSHS_schools_combined,
       'helper'      = read.csv('original-sources/helpers/county_isd_long.csv')),
  'tableau/district_school_reopening.xlsx',
  format_headers = FALSE
)

# COUNTY LEVEL --------------------------------------------------------------------------------------------

# TPR cpr --------------------------------------------------------------------------------------------
# 11/25 - csv no longer available
# get latest url
Clean_TPR = function(newest_file) {
  file_date = as.Date(str_match(newest_file, '\\d{8}'), '%Y%m%d')
  file_url  = paste0('https://beta.healthdata.gov', newest_file) %>%
    str_replace_all(.,
                    'https://beta.healthdata.govhttps://beta.healthdata.gov',
                    'https://beta.healthdata.gov') %>%
    str_replace_all(., ' ', '%20')

  temp = tempfile()
  download.file(file_url, temp, mode = 'wb')

  tpr_names =
    read_xlsx(temp, sheet = 6, skip = 0, n_max = 1) %>%
      dplyr::select(contains('TESTING: LAST WEEK')) %>%
      names()


  cpr_day = tpr_names %>%
    str_match(., '(\\d{1,2}),') %>%
    .[2] %>%
    as.numeric()

  cpr_month = tpr_names %>%
    str_extract_all(., 'January|February|March|April|May|June|July|August|September|October|November|December') %>%
    as.data.frame() %>%
    setNames('Date') %>%
    mutate(Date = as.Date(glue('{Date}-01-2022'), '%B-%d-%Y')) %>%
    arrange(desc(Date)) %>%
    slice(1) %>%
    pull(Date) %>%
    month()

  cpr_date = as.Date(glue('{cpr_month}/{cpr_day}'), '%m/%d')
  if (cpr_date - Sys.Date() > 100) {
    cpr_date = seq.Date(cpr_date, length.out = 2, by = '-1 year')[2]
  }

  print(cpr_date)
  cpr_tpr = read_xlsx(temp, sheet = 6, skip = 1) %>%
    filter(`State Abbreviation` == 'TX') %>%
    select('County',
           contains('NAAT positivity rate - last 7 days'),
           contains('Total NAATs - last 7 days')) %>%
    filter(County != 'Unallocated, TX') %>%
    mutate(County = gsub(' County, TX', '', County)) %>%
    setNames(c('County', 'TPR_CPR', 'Tests')) %>%
    mutate(Date = cpr_date) %>%
    arrange(County)

  output        = list(cpr_tpr)
  names(output) = cpr_date
  return(output)
}

page     = read_html('https://beta.healthdata.gov/National/COVID-19-Community-Profile-Report/gqxm-d9w9')
file_loc = page %>%
  html_nodes('script') %>%
  html_text() %>%
  str_detect('\\.xlsx') %>%
  which()

file_text = page %>%
  html_nodes('script') %>%
  .[file_loc] %>%
  html_text() %>%
  str_replace(., '\n    var initialState =\n      ', '') %>%
  str_replace(., '\n   ;\n  ', '')

tpr_url = fromJSON(file_text)$view$attachments %>%
  filter(href %>% str_detect('.xlsx')) %>%
  distinct() %>%
  arrange(desc(name)) %>%
  slice(1) %>%
  pull(href) %>%
  unname()

tpr_results = map(tpr_url, ~Clean_TPR(.)) %>%
  unlist(., recursive = FALSE)

map(names(tpr_results), ~fwrite(tpr_results[[.]],
                                glue('original-sources/historical/cpr/cpr_tpr_{.}.csv'),
                                quote = TRUE))

# combine cpr archive
all_cpr_tpr = rbindlist(lapply(list.files('original-sources/historical/cpr/', full.names = TRUE), read.csv)) %>%
  rename(TPR = TPR_CPR) %>%
  mutate(Date = as.Date(Date)) %>%
  mutate(TPR = ifelse(is.na(TPR), 0, TPR)) %>%
  mutate(Tests = as.integer(Tests))

# Google mobility --------------------------------------------------------------------------------------------
mobility_texas = fread('https://raw.githubusercontent.com/jeffbrennan/TexasPandemics/master/tableau/county.csv') %>%
  select(Date, County, Retail_Recreation:Residential) %>%
  distinct() %>%
  mutate(Date = as.Date(Date))

# vitals --------------------------------------------------------------------------------------------
## cases --------------------------------------------------------------------------------------------
new_case_url = "https://www.dshs.texas.gov/sites/default/files/chs/data/COVID/Texas%20COVID-19%20New%20Confirmed%20Cases%20by%20County.xlsx"
temp         = tempfile()
curl::curl_download(new_case_url, temp, mode = 'wb')
sheet_names = readxl::excel_sheets(temp)

all_cases = map(sheet_names, ~readxl::read_xlsx(temp, sheet = ., col_types = 'text', skip = 2))

DSHS_cases_long = all_cases %>%
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

max_case_date = max(DSHS_cases_long$Date, na.rm = TRUE)

## deaths --------------------------------------------------------------------------------------------
death_url = 'https://www.dshs.texas.gov/sites/default/files/chs/data/COVID/Texas%20COVID-19%20Fatality%20Count%20Data%20by%20County.xlsx'

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
  mutate(Cases_Daily_Imputed       = NA,
         Deaths_Daily_Imputed      = NA,
         Cases_Cumulative_Imputed  = NA,
         Deaths_Cumulative_Imputed = NA) %>%
  distinct()

stopifnot(
  DSHS_vitals_long %>%
    filter(Cases_Daily < 0) %>%
    nrow() == 0
)

stopifnot(DSHS_vitals_long %>%
            group_by(County, Date) %>%
            filter(n() > 1) %>%
            nrow()
            == 0
)
## merge data --------------------------------------------------------------------------------------------
county_tests = all_cpr_tpr %>%
  select(County, Date, Tests) %>%
  rename(Tests_Daily = Tests) %>%
  filter(!is.na(Tests_Daily)) %>%
  group_by(County) %>%
  arrange(Date) %>%
  mutate(Tests_Cumulative = cumsum(Tests_Daily))

merged_dshs_header = fread('tableau/county.csv', nrows = 0) %>% names()

merged_dshs_header = append(merged_dshs_header,
                            c('Cases_Cumulative_Imputed', 'Cases_Daily_Imputed',
                              'Deaths_Cumulative_Imputed', 'Deaths_Daily_Imputed'),
                            after = 6) %>%
  unique()

merged_dshs = DSHS_vitals_long %>%
  filter(County %in% unique(county_classifications$County)) %>%
  left_join(tsa_long_complete, by = 'County') %>%
  left_join(dshs_pops, by = 'County') %>%
  left_join(county_classifications, by = 'County') %>%
  left_join(county_tests %>% mutate(Date = as.Date(Date)),
            by = c('County', 'Date')) %>%
  mutate(TSA_Combined = str_c(TSA, ' - ', TSA_Name),
         PHR_Combined = str_c(PHR, ' - ', PHR_Name)
  ) %>%
  left_join(mobility_texas, by = c('Date', 'County')) %>%
  filter(County %in% unique(county_classifications$County)) %>%
  mutate(Population_DSHS = as.numeric(Population_DSHS)) %>%
  filter(Date >= as.Date('2020-03-06') & !is.na(County)) %>%
  distinct() %>%
  arrange(County, Date) %>%
  mutate(Active_Cases_Cumulative = NA,
         Active_Cases_Daily      = NA) %>%
  select(all_of(merged_dshs_header))

# diagnostic --------------------------------------------------------------------------------------------
stopifnot(merged_dshs$County %>% unique() %>% length() == 254)
stopifnot(merged_dshs$TSA_Combined %>%
            unique() %>%
            length() == 22)
fwrite(merged_dshs %>% arrange(County, Date), 'tableau/county.csv')

# TPR --------------------------------------------------------------------------------------------
tpr_df = rbindlist(
  lapply(list.files('original-sources/historical/cms_tpr/', full.names = TRUE), read.csv),
  fill = TRUE) %>%
  mutate(Date = as.Date(Date)) %>%
  rename(TPR = TPR_CMS) %>%
  mutate(TPR = ifelse(Date >= '2020-12-16', NA, TPR))

# add cms archive (8/19 - 12/09) (data represented 2 weeks of cases)
cpr_dates = list.files('original-sources/historical/cpr') %>%
  gsub('cpr_tpr_', '', .) %>%
  gsub('.csv', '', .) %>%
  as.Date(.)

cms_dates = list.files('original-sources/historical/cms_tpr/') %>%
  gsub('TPR_', '', .) %>%
  gsub('.csv', '', .) %>%
  as.Date(.)

TPR_dates     = sort(unique(c(cpr_dates, cms_dates)))
TPR_all_dates = data.frame(
  County = rep(county_classifications$County %>% unique(), each = length(TPR_dates)),
  Date   = rep(TPR_dates, times = length(county_classifications$County %>% unique())))

cms_archive = tpr_df %>%
  filter(Date < '2020-12-16') %>%
  select(Date, County, Tests, TPR)

cms_new = tpr_df %>%
  filter(Date >= '2020-12-16') %>%
  rbind(TPR_all_dates %>%
          filter(Date >= '2020-12-16'),
        fill = TRUE) %>%
  distinct() %>%
  select(Date, County, Tests) %>%
  filter(!is.na(Tests))

# divide case total by 7
tpr_cases = merged_dshs %>%
  dplyr::select(County, Date, Cases_Daily, Population_DSHS) %>%
  filter(Date >= as.Date(min(TPR_dates)) - 13 & Date <= max(TPR_dates)) %>%
  group_by(County) %>%
  mutate(Cases_100K_7Day_MA = (rollmean(Cases_Daily, k = 7, align = 'right',
                                        na.pad         = TRUE, na.rm = TRUE)
    / Population_DSHS) * 100000) %>%
  mutate(Cases_100K_14Day_MA = (rollmean(Cases_Daily, k = 14, align = 'right',
                                         na.rm          = TRUE, na.pad = TRUE)
    / Population_DSHS) * 100000) %>%
  filter(Date %in% TPR_dates) %>%
  dplyr::select(-Cases_Daily, -Population_DSHS, -Cases_100K_14Day_MA)

tpr_cases$Date %>% unique() %>% sort()

tpr_out = all_cpr_tpr %>%
  rbind(TPR_all_dates %>% filter(Date >= '2020-12-16'), fill = TRUE) %>%
  distinct() %>%
  arrange(County, Date) %>%
  rbind(cms_archive) %>%
  left_join(cms_new, by = c('County', 'Date')) %>%
  left_join(tpr_cases, by = c('County', 'Date')) %>%
  mutate(Tests.x = ifelse(is.na(Tests.y), Tests.x, Tests.y)) %>%
  rename(Tests = Tests.x) %>%
  dplyr::select(-Tests.y) %>%
  dplyr::select(County, Date, TPR, Tests, Cases_100K_7Day_MA) %>%
  arrange(County, Date) %>%
  group_by(County, Date) %>%
  slice(1) %>%
  distinct() %>%
  group_by(Date) %>%
  mutate(count_0 = sum(TPR == 0)) %>%
  ungroup() %>%
  filter(count_0 < 254 | is.na(count_0))
tpr_out$Date %>% unique() %>% sort()

fwrite(tpr_out, 'tableau/county_TPR.csv')

# vaccinations --------------------------------------------------------------------------------------------
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

# vaccine providers --------------------------------------------------------------------------------------------
curl::curl_download('https://genesis.soc.texas.gov/files/accessibility/vaccineprovideraccessibilitydata.csv', mode = 'wb',
                    glue('original-sources/historical/vaccine-providers/vaccination-providers_{date_out}.csv')) %>%
  try(., silent = TRUE)

# vaccine dashboard --------------------------------------------------------------------------------------------
vax_dir       = 'original-sources/historical/vax-dashboard'
date_modified = list.files(glue('{vax_dir}/temp/allocation'), full.names = TRUE)[1] %>%
  file.info() %>%
  .[['ctime']] %>%
  as.Date(.)

if (format(date_out, '%A') == 'Wednesday' & (date_out - date_modified == -1)) {
  vax_allocation = rbindlist(lapply(list.files(glue('{vax_dir}/temp/allocation/'), full.names = TRUE),
                                    read.csv, fileEncoding = "UTF-16LE", sep = '\t'),
                             fill = TRUE) %>%
    filter(X != 'DosesAllocatedWindow along Week No' &
             X != 'SumDosesAllocated (copy)') %>%
    dplyr::select(-X) %>%
    reshape2::melt(id = c('Allocation.Week.Range', 'CountyNameDisplay', 'Dose.Number..group.')) %>%
    dplyr::select(-variable) %>%
    setNames(c('Date', 'County', 'Dose', 'Count')) %>%
    mutate(Count = ifelse(Count == '', NA, Count)) %>%
    mutate(Count = as.numeric(gsub(',', '', Count))) %>%
    na.omit() %>%
    mutate(County = gsub(' County', '', County)) %>%
    filter(County != 'Texas') %>%
    mutate(Date = as.Date(Date, '%m/%d/%Y')) %>%
    reshape(idvar = c('Date', 'County'), timevar = 'Dose', direction = 'wide') %>%
    rename('Doses_Allocated_1' = `Count.First Doses`,
           'Doses_Allocated_2' = `Count.Second Doses`) %>%
    group_by(Date, County) %>%
    mutate(Doses_Allocated_1 = sum(Doses_Allocated_1, `Count.Federal Programs, First Doses`, na.rm = TRUE)) %>%
    dplyr::select(Date, County, Doses_Allocated_1, Doses_Allocated_2)

  vax_admin = rbindlist(lapply(list.files(glue('{vax_dir}/temp/admin/'), full.names = TRUE),
                               read.csv, fileEncoding = "UTF-16LE", sep = '\t'),
                        fill = TRUE) %>%
    reshape2::melt(id = c('Week.Start.End', 'CountyNameDisplay')) %>%
    dplyr::select(-variable) %>%
    setNames(c('Date', 'County', 'Doses_Administered')) %>%
    mutate(Doses_Administered) %>%
    mutate(Doses_Administered = as.numeric(gsub(',', '', Doses_Administered))) %>%
    na.omit() %>%
    mutate(County = gsub(' County', '', County)) %>%
    mutate(Date = str_extract(Date, '\\- (.*)')) %>%
    mutate(Date = gsub('- ', '', Date)) %>%
    mutate(Date = as.Date(Date, '%m/%d/%Y'))

  ## FULL
  demo_archive = list.files(glue('{vax_dir}/archive/'), full.names = TRUE)

  age_archive  = rbindlist(lapply(demo_archive %>% .[str_detect(., 'age')], fread), fill = TRUE) %>% distinct()
  race_archive = rbindlist(lapply(demo_archive %>% .[str_detect(., 'race')], fread), fill = TRUE) %>% distinct()

  vax_age_full = rbindlist(lapply(list.files(glue('{vax_dir}/temp/demo_age_full/'), full.names = TRUE),
                                  read.csv, fileEncoding = "UTF-16LE", sep = '\t'),
                           fill = TRUE) %>%
    dplyr::select(X, X.1, X.3, People.Vaccinated) %>%
    reshape2::melt(id = c('X', 'X.1', 'X.3')) %>%
    dplyr::select(-variable) %>%
    setNames(c('Age', 'County', 'Gender', 'Fully_Vaccinated')) %>%
    mutate(Date = max(vax_admin$Date)) %>%
    mutate(County = gsub(' County', '', County)) %>%
    dplyr::select(Date, County, everything()) %>%
    mutate(Date = as.Date(Date)) %>%
    mutate(Fully_Vaccinated = as.numeric(gsub(',', '', Fully_Vaccinated))) %>%
    arrange(Date, County)

  Read_Race = function(x) {
    county_name = str_match(x, '\\/race_(.*)\\.csv')[2]
    x_out       = read.csv(x, fileEncoding = "UTF-16LE", sep = '\t') %>%
      mutate(County = county_name)
    return(x_out)
  }

  vax_race_full = rbindlist(lapply(list.files(glue('{vax_dir}/temp/demo_race_full/'), full.names = TRUE),
                                   Read_Race),
                            fill = TRUE) %>%
    setNames(c('Race', 'dump1', 'Fully_Vaccinated', 'dump2', 'County')) %>%
    mutate(Date = max(vax_admin$Date)) %>%
    dplyr::select(Date, County, Race, Fully_Vaccinated, -dump1, -dump2) %>%
    mutate(Date = as.Date(Date)) %>%
    mutate(Fully_Vaccinated = as.numeric(gsub(',', '', Fully_Vaccinated))) %>%
    arrange(Date, County)


  # PARTIAL
  vax_age_partial = rbindlist(lapply(list.files(glue('{vax_dir}/temp/demo_age_partial/'), full.names = TRUE),
                                     read.csv, fileEncoding = "UTF-16LE", sep = '\t'),
                              fill = TRUE) %>%
    dplyr::select(X, X.1, X.3, People.Vaccinated) %>%
    reshape2::melt(id = c('X', 'X.1', 'X.3')) %>%
    dplyr::select(-variable) %>%
    setNames(c('Age', 'County', 'Gender', 'At_Least_One_Vaccinated')) %>%
    mutate(Date = max(vax_admin$Date)) %>%
    mutate(County = gsub(' County', '', County)) %>%
    dplyr::select(Date, County, everything()) %>%
    mutate(Date = as.Date(Date)) %>%
    mutate(At_Least_One_Vaccinated = as.numeric(gsub(',', '', At_Least_One_Vaccinated))) %>%
    arrange(Date, County)


  vax_race_partial = rbindlist(lapply(list.files(glue('{vax_dir}/temp/demo_race_partial/'), full.names = TRUE),
                                      Read_Race),
                               fill = TRUE) %>%
    setNames(c('Race', 'dump1', 'At_Least_One_Vaccinated', 'dump2', 'County')) %>%
    mutate(Date = max(vax_admin$Date)) %>%
    dplyr::select(Date, County, Race, At_Least_One_Vaccinated, -dump1, -dump2) %>%
    mutate(Date = as.Date(Date)) %>%
    mutate(At_Least_One_Vaccinated = as.numeric(gsub(',', '', At_Least_One_Vaccinated))) %>%
    arrange(Date, County)

  vax_age = vax_age_full %>%
    left_join(vax_age_partial, by = c('County', 'Date', 'Age', 'Gender')) %>%
    plyr::rbind.fill(age_archive) %>%
    arrange(County, Date)

  vax_race = vax_race_full %>%
    left_join(vax_race_partial, by = c('County', 'Date', 'Race')) %>%
    plyr::rbind.fill(race_archive) %>%
    arrange(County, Date)

  pop_files = list.files('original-sources/historical/vaccinations/', full.names = TRUE)

  vax_pop_counts = read_xlsx(pop_files[length(pop_files)], sheet = 2) %>%
    dplyr::select(`County Name`, ends_with('5+'), ends_with('16+'), ends_with('65+'),
                  contains('Healthcare'), contains('Long-term'), contains('Medical Condition')) %>%
    relocate(contains('65+'), .after = contains('16+')) %>%
    setNames(c('County',
               'Population_Over_5', 'Population_Over_16', 'Population_Over_65',
               'Population_Phase_1A_Healthcare',
               'Population_Phase1A_Care_Residents',
               'Population_Phase_1B_Medical_Condition', 'Date')) %>%
    filter(County %in% (dshs_pops$County %>% unique()))

  vax_out = vax_allocation %>%
    full_join(vax_admin, by = c('Date', 'County')) %>%
    left_join(vax_pop_counts, by = 'County') %>%
    full_join(vax_age_partial %>%
                group_by(Date, County) %>%
                summarize(At_Least_One_Vaccinated = sum(At_Least_One_Vaccinated, na.rm = TRUE))) %>%
    full_join(vax_age_full %>%
                group_by(Date, County) %>%
                summarize(Fully_Vaccinated = sum(Fully_Vaccinated, na.rm = TRUE))) %>%
    dplyr::select(Date, County, Doses_Allocated_1, Doses_Allocated_2,
                  Doses_Administered, At_Least_One_Vaccinated, Fully_Vaccinated,
                  everything())


  fwrite(vax_out, 'tableau/county_vaccine.csv')
  fwrite(vax_age, 'tableau/demographics_vax_age.csv')
  fwrite(vax_race, 'tableau/demographics_vax_race.csv')

  fwrite(
    vax_age %>% filter(Date == max(Date)),
    glue('{vax_dir}/archive/{max(vax_admin$Date)}_demographics_vax_age.csv')
  )

  fwrite(
    vax_race %>% filter(Date == max(Date)),
    glue('{vax_dir}/archive/{max(vax_admin$Date)}_demographics_vax_race.csv')
  )
}

# vax dashboard new --------------------------------------------------------------------------------------------
dashboard_archive        = list.files('original-sources/historical/vaccinations', full.names = TRUE)
current_vaccination_file = fread('tableau/sandbox/county_daily_vaccine.csv')

NUM_DAYS  = 4
new_files = dashboard_archive[(length(dashboard_archive) - (NUM_DAYS - 1)):length(dashboard_archive)]

start_time          = Sys.time()
all_dashboard_files = lapply(new_files, read_excel_allsheets, add_date = TRUE, col_option = FALSE, skip_option = 0)
print(Sys.time() - start_time)

county_vaccinations =
  rbindlist(
    lapply(
      lapply(all_dashboard_files, `[[`, 'By County'),
      function(x) return(x %>%
                           setNames(slice(., 1)) %>%
                           slice(2:nrow(.)) %>%
                           rename('Date' = ncol(.)))
    )
    , fill = TRUE) %>%
    filter(`County Name` %in% (dshs_pops$County %>% unique())) %>%
    select(Date, `County Name`,
           `Total Doses Allocated`, `Vaccine Doses Administered`,
           `People Vaccinated with at least One Dose`, `People Fully Vaccinated`,
           ends_with('5+'), `Population, 16+`, `Population, 65+`) %>%
    relocate(ends_with('5+'), .before = ends_with('16+')) %>%
    relocate(ends_with('65+'), .after = ends_with('16+')) %>%
    setNames(
      c('Date', 'County', 'Doses_Allocated',
        'Doses_Administered', 'At_Least_One_Dose', 'Fully_Vaccinated',
        'Population_5', 'Population_16', 'Population_65')) %>%
    mutate_at(vars(-County, -Date), as.numeric) %>%
    mutate(
      Doses_Allocated_Per_5     = Doses_Allocated / Population_5,
      Doses_Allocated_Per_16    = Doses_Allocated / Population_16,
      Doses_Allocated_Per_65    = Doses_Allocated / Population_65,
      Doses_Administered_Per_5  = Doses_Administered / Population_5,
      Doses_Administered_Per_16 = Doses_Administered / Population_16,
      Doses_Administered_Per_65 = Doses_Administered / Population_65,
      Fully_Vaccinated_Per_5    = Fully_Vaccinated / Population_5,
      Fully_Vaccinated_Per_16   = Fully_Vaccinated / Population_16,
    )
population_12       = all_dashboard_files[[length(all_dashboard_files)]][[2]] %>%
  setNames(slice(., 1)) %>%
  setNames(str_replace_all(names(.), '\\r', '')) %>%
  select(`County Name`, `Population\n12+`) %>%
  filter(`County Name` %in% (dshs_pops$County %>% unique())) %>%
  rename(County = `County Name`, Population_12 = `Population\n12+`) %>%
  filter(!is.na(County))

county_vaccinations_out = county_vaccinations %>%
  left_join(tsa_long_complete %>% select(County, TSA_Combined)) %>%
  left_join(county_classifications %>% select(County, PHR_Combined, Metro_Area)) %>%
  select(-contains('_Per')) %>%
  left_join(county_demo_agesex %>%
              select(County, `Age Group`, Population_Total) %>%
              filter(`Age Group` %in% c('<16', '16+')) %>%
              group_by(County) %>%
              summarize(Population_Total = sum(Population_Total)) %>%
              arrange(-Population_Total)) %>%
  left_join(population_12) %>%
  relocate(Population_12, .before = Population_16) %>%
  relocate(Population_Total, .before = TSA_Combined) %>%
  group_by(County) %>%
  mutate(Population_5 = coalesce(Population_5, NA)) %>%
  tidyr::fill(Population_5, .direction = 'updown') %>%
  plyr::rbind.fill(current_vaccination_file) %>%
  arrange(Date, County) %>%
  filter(TSA_Combined != '') %>%
  distinct()

stopifnot(county_vaccinations_out %>%
            group_by(County, Date) %>%
            filter(n() > 1) %>%
            nrow() == 0)
fwrite(county_vaccinations_out, 'tableau/sandbox/county_daily_vaccine.csv')

# state  --------------------------------------------------------------------------------------------
state_demo = lapply(all_dashboard_files, `[[`, 'By Age, Gender, Race') %>%
  discard(is.null) %>%
  lapply(., function(x) x %>%
    setNames(slice(., 1) %>% unlist()) %>%
    rename(Date = ncol(.)) %>%
    slice(2:nrow(.))) %>%
  rbindlist(fill = TRUE) %>%
  mutate(`Race/Ethnicity` = ifelse(is.na(`Race/Ethnicity`), str_c(Race, Ethnicity), `Race/Ethnicity`)) %>%
  select(-any_of(c('Race', 'Ethnicity', 'People Vaccinated with at Least One Dose', 'People Vaccinated'))) %>%
  unite(Age_Group, starts_with('Age'), remove = TRUE, na.rm = TRUE) %>%
  unite(Booster, contains('Booster'), remove = TRUE, na.rm = TRUE) %>%
  rename(
    any_of(
      c(
        Doses_Administered      = 'Doses Administered',
        At_Least_One_Vaccinated = 'People Vaccinated with at least One Dose',
        Fully_Vaccinated        = 'People Fully Vaccinated',
        Fully_Vaccinated        = 'People Fully Vaccinated ',
        Gender                  = 'Gender '
      )
    )
  ) %>%
  filter(Age_Group != 'Total') %>%
  mutate(Age_Group = str_replace_all(Age_Group, 'yr', '')) %>%
  mutate(Age_Group = str_replace_all(Age_Group, 'mo', ' months')) %>%
  mutate(Age_Group = ifelse(Age_Group %in% c('44692', '45057'), '5-11 years', Age_Group)) %>%
  mutate(Age_Group = ifelse(Age_Group %in% c('45275', '44910'), '12-15 years', Age_Group)) %>%
  mutate(Age_Group = ifelse(!str_detect(Age_Group, 'years|Unknown'), glue('{Age_Group} years'), Age_Group)) %>%
  mutate(Age_Group = str_squish(Age_Group)) %>%
  # remove dupes
  arrange(Date) %>%
  group_by(Gender, Age_Group, `Race/Ethnicity`) %>%
  mutate(dupe_val = Fully_Vaccinated == lag(Fully_Vaccinated)) %>%
  group_by(Date) %>%
  mutate(dupe_count = sum(dupe_val, na.rm = TRUE)) %>%
  mutate(row_count = n()) %>%
  ungroup() %>%
  filter(dupe_count != row_count) %>%
  filter(Date == max(Date)) %>%
  select(-dupe_count, -row_count, -dupe_val) %>%
  # add pops
  left_join(county_demo_race %>%
              group_by(`Race/Ethnicity`) %>%
              summarize(State_Race_Total = sum(Population_Total, na.rm = TRUE))) %>%
  left_join(county_demo_agesex %>%
              filter(`Age Group` == '<16' | `Age Group` == '16+') %>%
              group_by(Gender) %>%
              summarize(State_Gender_Total = sum(Population_Total, na.rm = TRUE))) %>%
  left_join(county_demo_agesex %>%
              group_by(`Age Group`) %>%
              summarize(State_Age_Total = sum(Population_Total, na.rm = TRUE)) %>%
              rename('Age_Group' = `Age Group`)) %>%
  mutate_at(vars(-Gender, -Age_Group, -`Race/Ethnicity`), as.numeric) %>%
  mutate(Doses_Administered_Per_Race        = Doses_Administered / State_Race_Total,
         Doses_Administered_Per_Gender      = Doses_Administered / State_Gender_Total,
         Doses_Administered_Per_Age         = Doses_Administered / State_Age_Total,
         At_Least_One_Vaccinated_Per_Race   = At_Least_One_Vaccinated / State_Race_Total,
         At_Least_One_Vaccinated_Per_Gender = At_Least_One_Vaccinated / State_Gender_Total,
         At_Least_One_Vaccinated_Per_Age    = At_Least_One_Vaccinated / State_Age_Total,
         Fully_Vaccinated_Per_Race          = Fully_Vaccinated / State_Race_Total,
         Fully_Vaccinated_Per_Gender        = Fully_Vaccinated / State_Gender_Total,
         Fully_Vaccinated_Per_Age           = Fully_Vaccinated / State_Age_Total,
         Booster_Per_Race                   = Booster / State_Race_Total,
         Booster_Per_Gender                 = Booster / State_Gender_Total,
         Booster_Per_Age                    = Booster / State_Age_Total) %>%
  relocate(`Race/Ethnicity`, .after = Age_Group) %>%
  mutate(Date = format(as.Date(Date, origin = '1970-01-01'), '%Y-%m-%d')) %>%
  relocate(Date, .after = Booster_Per_Age)

fwrite(state_demo, 'tableau/sandbox/state_vaccine_demographics.csv')

measure_cols       = c('Doses_Administered', 'At_Least_One_Vaccinated', 'Fully_Vaccinated')
state_demo_stacked =
  state_demo %>%
    select(c(contains('Gender'), measure_cols, -contains('Per'))) %>%
    mutate(Group_Type = 'Gender') %>%
    rename(Group            = Gender,
           Population_Total = State_Gender_Total) %>%
    group_by(Group_Type, Group, Population_Total) %>%
    summarize_at(measure_cols, sum, na.rm = TRUE) %>%
    rbind(state_demo %>%
            select(c(contains('Race'), measure_cols, -contains('Per'))) %>%
            mutate(Group_Type = 'Race') %>%
            rename(Group            = `Race/Ethnicity`,
                   Population_Total = State_Race_Total)) %>%
    group_by(Group_Type, Group, Population_Total) %>%
    summarize_at(measure_cols, sum, na.rm = TRUE) %>%
    rbind(state_demo %>%
            select(c(contains('Age'), measure_cols, -contains('Per'))) %>%
            mutate(Group_Type = 'Age') %>%
            rename(Group            = Age_Group,
                   Population_Total = State_Age_Total)) %>%
    group_by(Group_Type, Group, Population_Total) %>%
    summarize_at(measure_cols, sum, na.rm = TRUE) %>%
    relocate(Group_Type, .before = Group)

state_demo_stacked_clean =
  state_demo_stacked %>%
    # rbind(under_12) %>%
    arrange(Group, Group_Type) %>%
    mutate(Population_Total =
             ifelse(Group == '65-79 years & 80+ years',
                    county_demo_agesex %>%
                      filter(`Age Group` %in% c('65-79 years', '80+ years')) %>%
                      summarize(Population_Total = sum(Population_Total)) %>%
                      select(Population_Total) %>%
                      unlist(),
                    Population_Total)) %>%
    # pop16
    left_join(county_demo_race_age_15 %>%
                group_by(`Race/Ethnicity`) %>%
                summarize(Population_16 = sum(Population_16)),
              by = c('Group' = 'Race/Ethnicity')) %>%
    mutate(Population_16 = ifelse(Group_Type == 'Age', Population_Total, Population_16)) %>%
    mutate(Population_16 = ifelse(Group == '< 16 years', 0, Population_16)) %>%
    left_join(county_demo_agesex %>%
                filter(`Age Group` == '16+') %>%
                group_by(Gender) %>%
                summarize(Population_16 = sum(Population_Total)),
              by = c('Group' = 'Gender')) %>%
    mutate(Population_16 = ifelse(!is.na(Population_16.y), Population_16.y, Population_16.x)) %>%
    select(-contains('.x'), -contains('.y')) %>%
    relocate(Population_16, .after = Population_Total) %>%
    # pop 5
    left_join(county_demo_race_age_5 %>%
                group_by(`Race/Ethnicity`) %>%
                summarize(Population_5 = sum(Population_5)),
              by = c('Group' = 'Race/Ethnicity')) %>%
    mutate(Population_5 = ifelse(Group_Type == 'Age', Population_Total, Population_5)) %>%
    left_join(county_demo_agesex %>%
                filter(`Age Group` %in% c('5-11 years', '12-15 years', '16+')) %>%
                group_by(Gender) %>%
                summarize(Population_5 = sum(Population_Total)),
              by = c('Group' = 'Gender')) %>%
    mutate(Population_5 = ifelse(!is.na(Population_5.y), Population_5.y, Population_5.x)) %>%
    select(-contains('.x'), -contains('.y')) %>%
    relocate(Population_5, .after = Fully_Vaccinated) %>%
    arrange(Group_Type)

fwrite(state_demo_stacked_clean, 'tableau/sandbox/stacked_state_vaccine_demographics.csv')

county_vax_race = lapply(all_dashboard_files, `[[`, 'By County, Race') %>%
  discard(is.null) %>%
  .[[length(.)]] %>%
  setNames(slice(., 1)) %>%
  slice(2:nrow(.)) %>%
  select(1:(ncol(.) - 1)) %>%
  setNames(c('County', 'Race/Ethnicity',
             'At_Least_One_Vaccinated', 'Fully_Vaccinated', 'Booster', 'Doses_Administered')) %>%
  left_join(county_demo_race) %>%
  mutate_at(vars(-County, -`Race/Ethnicity`), as.numeric) %>%
  mutate(Doses_Administered_Per_Race = Doses_Administered / Population_Total) %>%
  mutate(At_Least_One_Vaccinated_Per_Race = At_Least_One_Vaccinated / Population_Total) %>%
  mutate(Fully_Vaccinated_Per_Race = Fully_Vaccinated / Population_Total) %>%
  mutate(Booster_Per_Race = Booster / Population_Total) %>%
  filter(!(County %in% c('Other', 'Grand Total'))) %>%
  select(-contains('_Per'))

fwrite(county_vax_race, 'tableau/sandbox/county_vax_race.csv')

county_vax_age = lapply(all_dashboard_files, `[[`, 'By County, Age') %>%
  discard(is.null) %>%
  .[[length(.)]] %>%
  setNames(slice(., 1)) %>%
  slice(2:nrow(.)) %>%
  select(1:(ncol(.) - 1)) %>%
  rename(any_of(c(`Age Group` = 'Agegrp',
                  `Age Group` = 'Agegrp (group) 1',
                  `Age Group` = 'Agegrp_v2'
  ))
  ) %>%
  mutate(`Age Group` = ifelse(`Age Group` == '44692', '5-11 years', `Age Group`)) %>%
  mutate(`Age Group` = ifelse(`Age Group` == '44910', '12-15 years', `Age Group`)) %>%
  setNames(c('County', 'Age_Group',
             'Doses_Administered', 'At_Least_One_Vaccinated', 'Fully_Vaccinated', 'Booster')) %>%
  mutate(Age_Group = glue('{Age_Group} years')) %>%
  mutate(Age_Group = gsub('years years', 'years', Age_Group)) %>%
  mutate(Age_Group = gsub('Unknown years', 'Unknown', Age_Group)) %>%
  mutate(Age_Group = str_squish(Age_Group)) %>%
  left_join(county_demo_agesex %>%
              group_by(County, `Age Group`) %>%
              summarize(Population_Total = sum(Population_Total, na.rm = TRUE)) %>%
              rename('Age_Group' = `Age Group`)) %>%
  mutate_at(vars(-County, -`Age_Group`), as.numeric) %>%
  mutate(Doses_Administered_Per_Age = Doses_Administered / Population_Total) %>%
  mutate(At_Least_One_Vaccinated_Per_Age = At_Least_One_Vaccinated / Population_Total) %>%
  mutate(Fully_Vaccinated_Per_Age = Fully_Vaccinated / Population_Total) %>%
  mutate(Booster_Per_Age = Booster / Population_Total) %>%
  filter(!(County %in% c('Other', 'Grand Total'))) %>%
  select(-contains('_Per'))

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

# longitudinal google data (mean)
DSHS_tsa_google =
  merged_dshs %>%
    distinct(
      Date, TSA, TSA_Name,
      Retail_Recreation, Grocery_Pharmacy,
      Parks, Transit,
      Workplaces, Residential
    ) %>%
    group_by(TSA, Date) %>%
    arrange(Retail_Recreation, Grocery_Pharmacy, Parks, Transit, Workplaces, Residential) %>%
    slice(1) %>%
    ungroup()

# DSHS_tsa = merge(DSHS_tsa_counts, DSHS_tsa_google, by = c('Date', 'TSA', 'TSA_Name'))
# DSHS_tsa = merge(DSHS_tsa, DSHS_tsa_pops, by = 'TSA', all = TRUE)
DSHS_tsa = DSHS_tsa_counts %>%
  left_join(DSHS_tsa_google, by = c('Date', 'TSA', 'TSA_Name')) %>%
  left_join(DSHS_tsa_pops, by = 'TSA')
# hospitals --------------------------------------------------------------------------------------------
DSHS_hosp_clean = function(df, var_name) {
  names(df) = df[1,]
  df_clean  = df %>%
    setNames(c('TSA ID', names(df)[2:length(names(df))])) %>%
    slice(2:23) %>%
    select(1, 3:ncol(.)) %>%
    mutate(`TSA ID` = gsub('.', '', `TSA ID`, fixed = TRUE))

  if (length(grep('.x', names(df_clean)) > 0)) {
    df_clean        = df_clean[, -grep('.x', names(df_clean))]
    names(df_clean) = gsub('.y', '', names(df_clean))
  }

  dates                                      = names(df_clean)[2:length(names(df_clean))]
  clean_dates                                = Date_Parser(dates)
  names(df_clean)[2:length(names(df_clean))] = clean_dates

  df_long        = reshape::melt(df_clean, id = 'TSA ID')
  names(df_long) = c('TSA', 'Date', var_name)
  df_long$Date   = as.Date(df_long$Date)

  if (length(which(df_long$Date == '2008-08-08')) > 0) {
    df_long$Date[which(df_long$Date == '2008-08-08')] = as.Date('2020-08-08')
  }

  return(df_long)
}

# TSA excel sheet --------------------------------------------------------------------------------------------
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

#
# TSA_HOSP_QUERY      = 'https://services3.arcgis.com/vljlarU2635mITsl/ArcGIS/rest/services/covid19_tsa_data_hosted/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=tsa%2Ctotal_lab_confirmed%2Cped_lab_confirmed_inpatient%2Ctotal_adult_lab_confirmed%2Ctotal_beds_occupied%2Ctotal_adult_icu%2Ctotal_ventilators_available%2Ctotal_lab_confirmed%2Ctotal_lab_confirmed_24hrs%2Ctotal_beds_available%2Cavailable_staffed_icu%2Cavailable_staffed_picu&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token='
# TSA_HOSP_DATE_QUERY = "https://services3.arcgis.com/vljlarU2635mITsl/ArcGIS/rest/services/covid19_tsa_data_hosted/FeatureServer/layers?f=pjson"
# TSA_HOSP_DATE_NUM   = fromJSON(TSA_HOSP_DATE_QUERY)$
#   layers$
#   editingInfo$
#   lastEditDate
#
# TSA_HOSP_DATE = as.POSIXct(TSA_HOSP_DATE_NUM / 1000, origin = "1970-01-01") %>%
#   as.Date()
#
# hosp_cols_new_raw = fromJSON(TSA_HOSP_QUERY)$features$attributes
#
# hosp_cols_new = hosp_cols_new_raw %>%
#   mutate(Hospitalizations_General = total_lab_confirmed - total_adult_icu) %>%
#   # mutate(Hospitalizations_Pediatric = total_lab_confirmed - total_adult_lab_confirmed) %>%
#   rename(
#     c(
#       # "Hospitalizations_General"     = "Adult COVID-19 General",
#       # ICU----------------------------------------------------
#       "Beds_Occupied_ICU"            = "total_adult_icu",
#       "Beds_Available_ICU"           = "available_staffed_icu",
#       "Hospitalizations_ICU"         = "total_adult_icu",
#
#       # General------------------------------------------------
#       "Hospitalizations_Total"       = "total_lab_confirmed",
#       "Beds_Available_Total"         = "total_beds_available",
#       "Beds_Occupied_Total"          = "total_beds_occupied",
#
#       # Pediatric ---------------------------------------------
#       "Hospitalizations_Pediatric"   = "ped_lab_confirmed_inpatient",
#       "Pediatric_Beds_Available_ICU" = "available_staffed_picu",
#
#       # Other
#       "Hospitalizations_24"          = "total_lab_confirmed_24hrs",
#       "Ventilators_Available"        = "total_ventilators_available",
#       "TSA"                          = "tsa"
#     )
#   ) %>%
#   mutate(Date = TSA_HOSP_DATE)
#
# # View(hosp_cols_new)
# hosp_url = 'https://raw.githubusercontent.com/jeffbrennan/TexasPandemics/master/tableau/hospitalizations_tsa.csv'
#
# hosp_cols = fread(hosp_url, fill = TRUE) %>%
#   select(Date, TSA, Hospitalizations_Total:Hospitalizations_24) %>%
#   filter(str_detect(Date, '\\d{4}-\\d{2}-\\d{2}')) %>%
#   plyr::rbind.fill(hosp_cols_new) %>%
#   arrange(TSA, Date) %>%
#   mutate(Date = as.Date(Date)) %>%
#   distinct() %>%
#   group_by(Date, TSA) %>%
#   arrange(desc(Hospitalizations_Total)) %>%
#   slice(1) %>%
#   ungroup()

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


TSA_hosp_combined_cleaned %>%
  group_by(Date, TSA) %>%
  filter(n() > 1) %>%
  ungroup() %>%
  arrange(TSA, Date) %>%
  View()

fwrite(merged_tsa, file = 'tableau/hospitalizations_tsa.csv')

# state level --------------------------------------------------------------------------------------------
# Wave declarations
waves = list(
  data.frame(Wave = 'Wave 1', Date = seq(as.Date('2020-10-01'), as.Date('2021-06-30'), by = 'days')),
  data.frame(Wave = 'Wave 2', Date = seq(as.Date('2021-07-01'), as.Date('2021-12-11'), by = 'days')),
  data.frame(Wave = 'Wave 3', Date = seq(as.Date('2021-12-12'), Sys.Date(), by = 'days'))
) %>%
  rbindlist() %>%
  mutate(Date = as.Date(Date))

# tsa wave
wave_comparison_tsa = fread('tableau/county.csv') %>%
  mutate(Date = as.Date(Date)) %>%
  group_by(TSA_Combined, Date) %>%
  summarize(Cases_Daily  = sum(Cases_Daily, na.rm = TRUE),
            Deaths_Daily = sum(Deaths_Daily, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(fread('tableau/hospitalizations_tsa.csv') %>%
              mutate(Date = as.Date(Date)) %>%
              group_by(TSA_Combined, Date) %>%
              summarize(Hospitalizations_24 = sum(Hospitalizations_24, na.rm = TRUE)),
            by = c('Date' = 'Date', 'TSA_Combined' = 'TSA_Combined')) %>%
  left_join(waves, by = c('Date' = 'Date')) %>%
  filter(!is.na(Wave)) %>%
  group_by(TSA_Combined, Wave) %>%
  summarize(Hospitalizations_24 = sum(Hospitalizations_24, na.rm = TRUE),
            Cases_Total         = sum(Cases_Daily, na.rm = TRUE)) %>%
  mutate(Hospitalizations_24_Ratio = Hospitalizations_24 / Cases_Total) %>%
  mutate(Level_Type = 'TSA') %>%
  rename(Level = TSA_Combined) %>%
  relocate(Level_Type, .before = Level)

wave_comparison_county = tsa_long_complete %>%
  select(County, TSA_Combined) %>%
  left_join(wave_comparison_tsa %>% select(-Level_Type), by = c('TSA_Combined' = 'Level')) %>%
  select(-TSA_Combined) %>%
  rename(Level = County) %>%
  mutate(Level_Type = 'County')

wave_comparison = wave_comparison_tsa %>%
  plyr::rbind.fill(
    wave_comparison_tsa %>%
      group_by(Wave) %>%
      mutate(Hospitalizations_24 = sum(Hospitalizations_24, na.rm = TRUE),
             Cases_Total         = sum(Cases_Total, na.rm = TRUE)) %>%
      mutate(Hospitalizations_24_Ratio = Hospitalizations_24 / Cases_Total) %>%
      ungroup() %>%
      mutate(Level_Type = 'State', Level = 'Texas') %>%
      distinct()
  ) %>%
  plyr::rbind.fill(wave_comparison_county)

fwrite(wave_comparison, 'tableau/wave_comparison.csv')

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
  mutate(across(everything(), ~gsub('\\%|\\,', '', .))) %>%
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

# dshs demographics --------------------------------------------------------------------------------------------
Clean_Demographics = function(sheet_name, file_date = date_out) {
  group_type = str_extract(sheet_name, 'Age|Gender|Race')
  stat_type  = str_extract(sheet_name, 'Case|Fatal')

  if (stat_type == 'Case') {
    out_df = weekly_demo[[sheet_name]] %>%
      as.data.frame() %>%
      dplyr::select(1:2) %>%
      setNames(c('Group', 'Cases_Cumulative')) %>%
      filter(!Group %in% c('Pending DOB', 'Total', 'Unknown', 'Grand Total')) %>%
      mutate(Date = as.Date(file_date)) %>%
      mutate(Group_Type = group_type) %>%
      group_by(Date) %>%
      mutate(Cases_PCT = Cases_Cumulative / sum(Cases_Cumulative)) %>%
      dplyr::select(Date, Group_Type, Group, Cases_Cumulative, Cases_PCT)

  } else if (stat_type == 'Fatal') {
    out_df = weekly_demo[sheet_name] %>%
      as.data.frame() %>%
      dplyr::select(1:2) %>%
      setNames(c('Group', 'Deaths_Cumulative')) %>%
      filter(!Group %in% c('Pending DOB', 'Total', 'Unknown', 'Grand Total')) %>%
      mutate(Date = as.Date(file_date)) %>%
      mutate(Group_Type = group_type) %>%
      group_by(Date) %>%
      mutate(Deaths_PCT = Deaths_Cumulative / sum(Deaths_Cumulative)) %>%
      dplyr::select(Date, Group_Type, Group, Deaths_Cumulative, Deaths_PCT)
  }
  return(out_df)
}


# dshs demographics new --------------------------------------------------------------------------------------------
Clean_Demo_New = function(sheet_name) {
  if (str_detect(sheet_name, 'Fatal|Confirmed')) {

    group_type = str_extract(sheet_name, 'Age|Sex|Gender|Race')
    stat_type  = str_extract(sheet_name, 'Cases|Fatalities')

    df = demographics_all[[sheet_name]]

    clean_df = df %>%
      slice(3:nrow(df)) %>%
      setNames(df[2,]) %>%
      select(-Total) %>%
      pivot_longer(!1) %>%
      setNames(c('Date', 'Group', stat_type)) %>%
      filter(!str_detect(Date, 'Total|Notes')) %>%
      mutate(Date = as.Date(glue('01 {Date}'), '%d %B %Y')) %>%
      mutate(Group_Type = group_type)

    return(clean_df)
  }
}

# monthly
demo_releases = seq(as.Date('2022-01-01'), by = 'month', length = 50) + days(14)

if (date_out %in% demo_releases) {
  demo_url = 'https://dshs.state.tx.us/coronavirus/TexasCOVID19Demographics_Counts.xlsx'
  curl::curl_download(demo_url, glue('original-sources/historical/demo-archive/demo_{date_out}.xlsx'))


  all_demo_files       = list.files('original-sources/historical/demo-archive', full.names = TRUE)
  monthly_update_dates = str_extract(all_demo_files, '\\d{4}-\\d{2}-\\d{2}') %>%
    as.Date() %>%
    .[which(. >= as.Date('2022-01-14'))]

  monthly_updates = sapply(monthly_update_dates,
                           function(x) str_detect(all_demo_files, as.character(x)) %>%
                             which(.) %>%
                             all_demo_files[.]
  )

  demographics_all = read_excel_allsheets(monthly_updates[length(monthly_updates)])

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
