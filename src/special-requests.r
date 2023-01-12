# setup ----
library(tidyverse)
library(data.table)
library(R0)
library(zoo)
library(ggpubr)
library(jcolors)
library(glue)
library(lubridate)

select = dplyr::select
set.seed(1)

# functions -----------------------------------------------------------------------------------
read_excel_allsheets = function(filename, col_option = TRUE, add_date = FALSE, skip_option = 1) {
  sheets   = readxl::excel_sheets(filename)
  x        = lapply(sheets, function(X) readxl::read_excel(filename, sheet = X,
                                                           skip            = skip_option,
                                                           col_names       = col_option, na = '.',
                                                           col_types       = 'text'
  ),
  )
  names(x) = sheets
  x        = lapply(x, as.data.frame)

  if (add_date == TRUE) {
    file_date = str_extract(filename, '\\d.*\\d')
    x         = lapply(x, function(z) return(mutate(z, Date = as.Date(file_date))))
  }
  return(x)
}

#Function to generate Rt estimates, need to read in data frame and population size
rt_df_extraction = function(Rt_estimate_output) {

  # extract r0 estimate values into dataframe
  rt_df      = setNames(stack(Rt_estimate_output$estimates$TD$R)[2:1], c('Date', 'Rt'))
  rt_df$Date = as.Date(rt_df$Date)

  # get 95% CI
  CI_lower_list = Rt_estimate_output$estimates$TD$conf.int$lower
  CI_upper_list = Rt_estimate_output$estimates$TD$conf.int$upper

  #use unlist function to format as vector
  CI_lower = unlist(CI_lower_list, recursive = TRUE, use.names = TRUE)
  CI_upper = unlist(CI_upper_list, recursive = TRUE, use.names = TRUE)

  rt_df$lower = CI_lower
  rt_df$upper = CI_upper

  rt_df = rt_df %>%
    mutate(lower = replace(lower, Rt == 0, NA)) %>%
    mutate(upper = replace(upper, Rt == 0, NA)) %>%
    mutate(Rt = replace(Rt, Rt == 0, NA))

  return(rt_df)
}

covid_rt = function(mydata, threshold) {
  set.seed(1)

  ### DECLARE VALS ###
  #set generation time
  #Tapiwa, Ganyani "Esimating the gen interval for Covid-19":

  # LOGNORMAL OPS
  # gen.time=generation.time("lognormal", c(4.0, 2.9))
  # gen.time=generation.time("lognormal", c(4.7,2.9)) #Nishiura

  # GAMMA OPS
  # gen.time=generation.time("gamma", c(5.2, 1.72)) #Singapore
  # gen.time=generation.time("gamma", c(3.95, 1.51)) #Tianjin
  gen.time = generation.time("gamma", c(3.96, 4.75))
  print(mydata %>%
          dplyr::select(2) %>%
          distinct() %>%
          unlist() %>%
          setNames(NULL))

  #change na values to 0
  mydata = mydata %>% mutate(Cases_Daily = ifelse(is.na(Cases_Daily), 0, Cases_Daily))

  # get case average from past month
  recent_case_avg = mydata %>%
    filter(Date > seq(max(Date), length = 2, by = "-1 month")[2]) %>%
    summarize(mean(Cases_Daily, na.rm = TRUE)) %>%
    unlist()

  print(round(recent_case_avg, 2))

  pop.DSHS       = mydata$Population_DSHS[1]
  #Get 7 day moving average of daily cases
  mydata$MA_7day = rollmean(mydata$Cases_Daily, k = 7, na.pad = TRUE, align = 'right')

  #create a vector of new cases 7 day moving average
  mydata.new = pull(mydata, MA_7day)
  #mydata.new = pull(mydata, Cases_Daily)

  # get dates as vectors
  date.vector = pull(mydata, Date)

  #create a named numerical vector using the date.vector as names of new cases
  #Note: this is needed to run R0 package function estimate.R()
  names(mydata.new) = c(date.vector)


  #get row number of March 15 and first nonzero entryrecen
  #NOTE: for 7 day moving average start March 15, for daily start March 9
  #find max row between the two (this will be beginning of rt data used)
  march15.row   = which(mydata$Date == "2020-03-15")
  first.nonzero = min(which(mydata$Cases_Daily > 0))
  last.nonzero  = max(which(mydata$Cases_Daily > 0))

  first.nonzero = ifelse(is.infinite(first.nonzero), NA, first.nonzero)
  last.nonzero  = ifelse(is.infinite(last.nonzero), NA, last.nonzero)

  minrow = max(march15.row, first.nonzero, na.rm = TRUE)
  maxrow = as.integer(min(last.nonzero, nrow(mydata), na.rm = TRUE))

  mydata = mydata %>% slice(minrow:maxrow)

  ### R0 ESTIMATION ###
  if (!is.na(minrow) & recent_case_avg > threshold) {

    #reduce the vector to be rows from min date (March 9 or first nonzero case) to current date
    mydata.newest = mydata.new[minrow:maxrow]

    tryCatch({
      rt.DSHS = estimate.R(mydata.newest,
                           gen.time,
                           begin    = as.integer(1),
                           end      = length(mydata.newest),
                           methods  = c("TD"),
                           pop.size = pop.DSHS,
                           nsim     = 1000)

      rt.DSHS.df = rt_df_extraction(rt.DSHS)
    },
      error = function(e) {
        writeLines(paste0('Rt generation error (despite sufficient cases)', '\n'))
        rt.DSHS.df = data.frame(Date  = as.Date(mydata$Date),
                                Rt    = rep(NA, length(mydata$Date)),
                                lower = rep(NA, length(mydata$Date)),
                                upper = rep(NA, length(mydata$Date)))
      })
  } else {
    writeLines(paste0('Below required case threshold of ', round(threshold, 2), '\n'))
    # error catch small regions & na values for minrow
    rt.DSHS.df = data.frame(Date  = as.Date(mydata$Date),
                            Rt    = rep(NA, length(mydata$Date)),
                            lower = rep(NA, length(mydata$Date)),
                            upper = rep(NA, length(mydata$Date)))
  }
  return(rt.DSHS.df)
}

Date_Parser = function(Date) {
  #Matches 2 or 4 digits, separator, 2 or 4 or 1 digits, optional separator, optional 2 or 4 or 1 digits
  # Coerces all common separators to "-" 
  # date_regex = '(\\d{4}|\\d{2})(\\.|\\-|\\/)(\\d{4}|\\d{2})?(\\.|\\-|\\/)?(\\d{4}|\\d{2})'
  date_regex = '(\\d{4}|\\d{2}|\\d{1})(\\.|\\-|\\/)(\\d{4}|\\d{2}|\\d{1})?(\\.|\\-|\\/)?(\\d{4}|\\d{2}|\\d{1})'

  if (!is.na(as.numeric(Date)[1])) {
    dates_out = sapply(Date, function(x) as.Date(as.numeric(x), origin = '1899-12-30'))

  } else {

    clean_dates       = str_extract(Date, date_regex) %>% str_replace_all(., '\\/|\\.', '\\-')
    clean_dates_no_na = clean_dates[!is.na(clean_dates)]


    mdy_dates = which(!is.na(as.Date(clean_dates, format = '%m-%d-%y')))
    md_dates  = which(!is.na(as.Date(clean_dates, format = '%m-%d')))
    if (length(mdy_dates) == length(md_dates)) {
      md_dates = c()
    }
    md_dates_2020                         = which(format(as.Date(clean_dates[md_dates], '%m-%d'), '%m') %in% (c('10', '11', '12')))
    clean_dates[md_dates[md_dates_2020]]  = paste0('2020-', clean_dates[md_dates[md_dates_2020]])
    clean_dates[md_dates[-md_dates_2020]] = format(as.Date(clean_dates[md_dates[-md_dates_2020]],
                                                           '%m-%d'), '%Y-%m-%d')
    dates_out                             = as.Date(clean_dates, '%m-%d-%Y')
    if (sum(!is.na(dates_out)) != length(clean_dates_no_na)) {
      dates_out = as.Date(clean_dates, '%Y-%m-%d')
    }
  }
  return(as_date(dates_out))
}

Duplicate_Check = function(df) {
  if (length(grep('\\.x', names(df)) > 0)) {
    df        = df[, -grep('\\.x', names(df))]
    names(df) = gsub('\\.y', '', names(df))
  }
  return(df)
}

# plot with shaded confidence intervals
focused_rt_plot = function(rt_data) {
  library(ggplot2)
  plot = rt_data %>%
    filter(Date >= seq(date_out, length = 2, by = "-2 month")[2]) %>%
    ggplot(., aes(Date, Rt)) +
    geom_ribbon(aes(ymin = lower, ymax = upper), fill = "gray80") +
    geom_hline(yintercept = 1, linetype = 'dashed', color = 'blue', size = 1) +
    geom_point(color = "black", size = 2) +
    labs(title = bquote('TMC' ~ R[t] ~ 'Estimate (Past 2 Months)')) +
    theme_pubr()
  return(plot)
}

# SETUP ---------------------------------------------------------------------------------------
# setwd('C:/Users/jeffb/Desktop/Life/personal-projects/COVID')

date_out = as.Date(ifelse((Sys.time() < as.POSIXct(paste0(Sys.Date(), '15:05'), tz = 'America/Chicago')),
                          Sys.Date() - 1,
                          Sys.Date()),
                   '1970-01-01')

county_pops           = unique(read.csv('https://raw.githubusercontent.com/jeffbrennan/COVID-19/d03d476f7fb060dfd2e1a600a6a1e449df0ab8df/original-sources/DSHS_county_cases.csv')[, c('County', 'Population')])
colnames(county_pops) = c('County', 'Population_DSHS')


# pull cases ----------------------------------------------------------------------------------
Impute_Vals = function(df, var_name) {
  values_out = df |>
    mutate(missing_days = sum(missing_day != 0)) |>
    tidyr::fill(!!var_name, .direction = 'down') |>
    mutate(val_diff = max(!!var_name, na.rm = TRUE) - min(!!var_name, na.rm = TRUE)) |>
    mutate(val_spread = val_diff / (missing_days + 1)) |>
    mutate(imputed_vals = !!var_name + (val_spread * missing_day)) |>
    mutate(imputed_vals = as.integer(floor(imputed_vals))) |>
    select(imputed_vals)
  pull(imputed_Vals)

  return(values_out)
}

Load_Vitals = function(fpath) {
  fdate = str_extract(fpath, '\\d{4}_\\d{2}_\\d{2}') %>%
    as.Date(., '%Y_%m_%d')

  result = readxl::read_xlsx(fpath, sheet = 'Case and Fatalities_ALL', skip = 1) |>
    select(County, `Confirmed Cases`, Fatalities) |>
    rename(Cases_Cumulative  = `Confirmed Cases`,
           Deaths_Cumulative = Fatalities) |>
    mutate(Date = fdate)
  return(result)
}

PREV_VITAL_PATH      = 'tableau/county.csv'
MISSING_VITALS_DATES = c('2022-08-25', '2022-09-15')
TMC_COUNTIES         = c('Austin', 'Brazoria', 'Chambers', 'Fort Bend',
                         'Galveston', 'Harris', 'Liberty', 'Montgomery', 'Waller')

CURRENT_RT_DATE = fread('special-requests/TMC/rt_estimate.csv') %>%
  pull(Date) %>%
  max()

# check new cases ------------------------------------------------------------------------------
# new_case_url = 'https://www.dshs.texas.gov/sites/default/files/STATEEPI-CHS/coronavirus/CaseCountData.xlsx'
# temp         = tempfile()
# curl::curl_download(new_case_url, temp, mode = 'wb')
#
# VITALS_DATE = readxl::read_xlsx(temp, sheet = 'Case and Fatalities_ALL') %>%
#   names() %>%
#   .[1] %>%
#   str_extract_all(., '\\d{2}/\\d{2}/\\d{4}') %>%
#   .[[1]] %>%
#   .[length(.)] %>%
#   as.Date(., '%m/%d/%Y')
#
# curl::curl_download(new_case_url,
#                     paste0('original-sources/historical/state/dshs_',
#                            format(VITALS_DATE, '%Y_%m_%d'), '.xlsx'),
#                     mode = 'wb')
#
#

#
# stopifnot(VITALS_DATE > CURRENT_RT_DATE)
#
# PREV_VITALS = fread(PREV_VITAL_PATH) |>
#   select(Date, County, Cases_Cumulative, Deaths_Cumulative,
#          Cases_Cumulative_Imputed, Deaths_Cumulative_Imputed,
#          Cases_Daily_Imputed, Deaths_Daily_Imputed) |>
#   mutate(Date = as.Date(Date)) |>
#   filter(Date < VITALS_DATE)
#
# REAL_COUNTIES = PREV_VITALS |>
#   filter(Date == '2021-01-01') |>
#   pull(County) |>
#   unique()
#
# ALL_VITAL_FILES = list.files('original-sources/historical/state', full.names = TRUE) %>%
#   .[!str_detect(., '_NA')]
#
# MISSING_FILE_POS = str_extract(ALL_VITAL_FILES, '\\d{4}_\\d{2}_\\d{2}') |>
#   unique() %>%
#   as.Date(., '%Y_%m_%d') %>%
#   .[which(. > max(PREV_VITALS$Date))] %>%
#   format('%Y_%m_%d')
#
# NEW_FILES = map_chr(MISSING_FILE_POS, ~ALL_VITAL_FILES[str_detect(ALL_VITAL_FILES, .)])
#
# DSHS_vitals_long_raw = map(NEW_FILES, Load_Vitals) |>
#   rbindlist(fill = TRUE) |>
#   plyr::rbind.fill(PREV_VITALS |> filter(Date < VITALS_DATE)) |>
#   distinct() |>
#   group_by(County) |>
#   arrange(Date)
#
#
# DSHS_vitals_long_impute_prep = DSHS_vitals_long_raw |>
#   filter(County %in% TMC_COUNTIES) |>
#   tidyr::complete(Date = seq.Date(min(Date), as.Date(VITALS_DATE), by = "day")) |>
#   mutate(Cases_Cumulative = ifelse(is.na(Cases_Cumulative) | Date %in% MISSING_VITALS_DATES,
#                                    Cases_Cumulative_Imputed,
#                                    Cases_Cumulative)) |>
#   mutate(Deaths_Cumulative = ifelse(is.na(Deaths_Cumulative) | Date %in% MISSING_VITALS_DATES,
#                                     Deaths_Cumulative_Imputed,
#                                     Deaths_Cumulative)) |>
#   mutate(Deaths_Daily = Deaths_Cumulative - lag(Deaths_Cumulative),
#          Cases_Daily  = Cases_Cumulative - lag(Cases_Cumulative)) |>
#   mutate(missing_day = cumsum(is.na(Cases_Cumulative) & !Date %in% MISSING_VITALS_DATES)) |>
#   mutate(missing_day = ifelse(missing_day == lag(missing_day) |
#                                 missing_day == 0 |
#                                 row_number() == 1, 0, missing_day)) |>
#   mutate(impute_group = missing_day != 0 |
#     lead(missing_day) != 0 |
#     lag(missing_day) != 0) |>
#   mutate(impute_group = ifelse(is.na(impute_group), FALSE, impute_group)) |>
#   mutate(impute_group2 = cumsum(as.integer(impute_group & lag(impute_group) == FALSE))) |>
#   mutate(impute_group2 = ifelse(impute_group == FALSE, 0, impute_group2))
#
# IMPUTED_CHECK = DSHS_vitals_long_impute_prep |>
#   filter(impute_group) |>
#   nrow() > 0
#
# if (IMPUTED_CHECK) {
#   message('PERFORMING IMPUTATION')
#   DSHS_vitals_long_imputed = DSHS_vitals_long_impute_prep |>
#     filter(impute_group) %>%
#     group_by(County, impute_group2) |>
#       mutate(missing_day = row_number() - 1) |>
#       mutate(missing_day = ifelse(Date == max(Date) | Date == min(Date), 0, missing_day)) |>
#       mutate(missing_days = sum(missing_day != 0)) |>
#       # cases
#       tidyr::fill(Cases_Cumulative, .direction = 'down') |>
#       mutate(Case_diff = max(Cases_Cumulative, na.rm = TRUE) - min(Cases_Cumulative, na.rm = TRUE)) |>
#       mutate(Cases_Daily_spread = Case_diff / (missing_days + 1)) |>
#       mutate(Cases_Cumulative_Imputed = Cases_Cumulative + (Cases_Daily_spread * missing_day)) |>
#       mutate(Cases_Cumulative_Imputed = as.integer(floor(Cases_Cumulative_Imputed))) |>
#       # deaths
#       tidyr::fill(Deaths_Cumulative, .direction = 'down') |>
#       mutate(Death_diff = max(Deaths_Cumulative, na.rm = TRUE) - min(Deaths_Cumulative, na.rm = TRUE)) |>
#       mutate(Deaths_Daily_spread = Death_diff / (missing_days + 1)) |>
#       mutate(Deaths_Cumulative_Imputed = Deaths_Cumulative + (Deaths_Daily_spread * missing_day)) |>
#       mutate(Deaths_Cumulative_Imputed = as.integer(floor(Deaths_Cumulative_Imputed))) |>
#       ungroup() |>
#       select(County, Date, contains('Cumulative_Imputed'))
#
#   DSHS_vitals_long =
#     DSHS_vitals_long_impute_prep |>
#       select(-contains('Imputed')) |>
#       left_join(DSHS_vitals_long_imputed, by = c('County', 'Date')) |>
#       filter(!is.na(County)) |>
#       select(-impute_group) |>
#       mutate(Cases_Cumulative_Imputed = ifelse(is.na(Cases_Cumulative_Imputed), Cases_Cumulative, Cases_Cumulative_Imputed)) |>
#       mutate(Deaths_Cumulative_Imputed = ifelse(is.na(Deaths_Cumulative_Imputed), Deaths_Cumulative, Deaths_Cumulative_Imputed)) |>
#       group_by(County) |>
#       mutate(Cases_Daily_Imputed = Cases_Cumulative_Imputed - lag(Cases_Cumulative_Imputed)) |>
#       mutate(Deaths_Daily_Imputed = Deaths_Cumulative_Imputed - lag(Deaths_Cumulative_Imputed)) |>
#       mutate(across(contains('Imputed'), as.integer)) |>
#       mutate(Cases_Cumulative = ifelse(is.na(Cases_Cumulative) | Date %in% MISSING_VITALS_DATES,
#                                        Cases_Cumulative_Imputed,
#                                        Cases_Cumulative)) |>
#       mutate(Deaths_Cumulative = ifelse(is.na(Deaths_Cumulative) | Date %in% MISSING_VITALS_DATES,
#                                         Deaths_Cumulative_Imputed,
#                                         Deaths_Cumulative)) |>
#       mutate(Deaths_Daily = Deaths_Cumulative - lag(Deaths_Cumulative),
#              Cases_Daily  = Cases_Cumulative - lag(Cases_Cumulative)) |>
#       ungroup()
#
#
# } else {
#   message('NO IMPUTATION NEEDED')
#   DSHS_vitals_long = DSHS_vitals_long_impute_prep |>
#     ungroup() |>
#     select(-impute_group, -missing_day)
# }

# clean ---------------------------------------------------------------------------------------

# new pull --------------------------------------------------------------------------------------------
# DSHS began uploading daily data again on 2022-12-14
new_case_url = "https://www.dshs.texas.gov/sites/default/files/chs/data/COVID/Texas%20COVID-19%20New%20Confirmed%20Cases%20by%20County.xlsx"
temp         = tempfile()
curl::curl_download(new_case_url, temp, mode = 'wb')
sheet_names = readxl::excel_sheets(temp)

all_cases = map(sheet_names, ~readxl::read_xlsx(temp, sheet = ., col_types = 'text', skip = 2))

DSHS_vitals_long = all_cases %>%
  rbindlist(fill = TRUE) %>%
  pivot_longer(!County) %>%
  filter(County %in% TMC_COUNTIES) %>%
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
  mutate(Cases_Daily = as.integer(Cases_Daily))

max_case_date = max(DSHS_vitals_long$Date, na.rm = TRUE)
date_diff     = difftime(max_case_date, CURRENT_RT_DATE, units = 'days')
# check cumulative file
if (date_diff <= 1) {
  temp         = tempfile()
  curl::curl_download(new_case_url, temp, mode = 'wb')
  sheet_names = readxl::excel_sheets(temp)

  all_cases = map(sheet_names, ~readxl::read_xlsx(temp, sheet = ., col_types = 'text', skip = 2))

  DSHS_vitals_long = all_cases %>%
    rbindlist(fill = TRUE) %>%
    pivot_longer(!County) %>%
    filter(County %in% TMC_COUNTIES) %>%
    filter(!is.na(value)) %>%
    rename(Date             = name,
           Cases_Cumulative = value) %>%
    filter(!str_to_upper(Date) %in% c("TOTAL", "UNKNOWN DATE")) %>%
    mutate(Date = ifelse(!is.na(as.integer(Date)),
                         as.character(as.Date(as.integer(Date), origin = '1899-12-30')),
                         as.character(as.Date(Date, format = '%m/%d/%Y'))
    )
    ) %>%
    mutate(Date = as.Date(Date)) %>%
    mutate(Cases_Cumulative = as.integer(Cases_Cumulative)) %>%
    arrange(Date) %>%
    group_by(County) %>%
    mutate(Cases_Daily = Cases_Cumulative - lag(Cases_Cumulative)) %>%
    mutate(Cases_Daily = ifelse(is.na(Cases_Daily) | Cases_Daily < 0, 0, Cases_Daily)) %>%
    filter(!is.na(Date)) %>%
    select(-Cases_Cumulative)
}

max_case_date = max(DSHS_vitals_long$Date, na.rm = TRUE)
date_diff     = difftime(max_case_date, CURRENT_RT_DATE, units = 'days')

stopifnot(date_diff > 1)

# run RT on TMC ----------------------------------------------------------------------------------
TMC = DSHS_vitals_long %>%
  select(County, Date, Cases_Daily) |>
    ungroup() |>
    filter(County %in% TMC_COUNTIES) %>%
  left_join(county_pops, by = 'County') %>%
  mutate(Population_DSHS = as.numeric(Population_DSHS)) |>
    dplyr::select(-County) %>%
  melt(id = "Date") %>%
  dcast(Date ~ variable, sum, na.rm = TRUE)

# run covid_rt function on count.focus.cast dataframe
TMC_rt_output = covid_rt(TMC, threshold = 0) %>% na.omit()

# viz results ---------------------------------------------------------------------------------
TMC_contributions = DSHS_vitals_long %>%
  mutate(Date = as.Date(Date)) %>%
  group_by(Date) %>%
  mutate(TMC_cases = sum(Cases_Daily)) %>%
  mutate(County_prop = Cases_Daily / TMC_cases) %>%
  ungroup() %>%
  group_by(County) %>%
  mutate(Cases_MA_7 = round(rollmean(TMC_cases, k = 7, na.pad = TRUE, align = 'right'), 2)) %>%
  mutate(PCT_change = Cases_Daily / lag(Cases_Daily)) %>%
  dplyr::select(Date, County, Cases_Daily, Cases_MA_7, TMC_cases, County_prop, PCT_change) %>%
  filter(Date > Sys.Date() - 15)


TMC_plot = focused_rt_plot(TMC_rt_output)

# diagnostics ----
stacked_plot = ggplot(TMC_contributions, aes(x = Date, y = Cases_Daily)) +
  geom_bar(aes(fill = County), stat = 'identity') +
  geom_line(aes(x = Date, y = Cases_MA_7, color = '7-Day Moving Average'), size = 1) +
  geom_point(aes(x = Date, y = Cases_MA_7, color = '7-Day Moving Average'), size = 2) +
  geom_label(aes(label = stat(y), group = Date),
             stat = 'summary', fun = sum, vjust = -0.2, size = 4) +
  labs(y = 'Daily Cases', title = 'TMC County Cases (Past 2 Weeks)') +
  scale_x_date(date_breaks = '1 day', date_labels = '%m/%d', expand = c(0, 0)) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.1))) +
  scale_color_manual(values = c('7-Day Moving Average' = 'black')) +
  scale_fill_jcolors('rainbow') +
  guides(color = guide_legend(order = 1),
         fill  = guide_legend(order = 2)) +
  theme_pubr() +
  theme(legend.position = 'right',
        legend.title    = element_blank(),
        axis.text.x     = element_text(angle = -45))


county_plot = ggplot(TMC_contributions, aes(x = Date, y = Cases_Daily, fill = County, group = County)) +
  geom_bar(stat = 'identity') +
  labs(y = 'Daily Cases') +
  geom_text(aes(label = stat(y), group = Date),
            stat = 'summary', fun = sum, vjust = -0.05) +
  scale_x_date(date_breaks = '1 day', date_labels = '%m/%d', expand = c(0, 0)) +
  facet_wrap(~County, ncol = 3) +
  scale_fill_jcolors('rainbow') +
  theme_pubr(border = TRUE) +
  theme(legend.position = 'none',
        axis.text.x     = element_text(angle = -90))

recent_rt = TMC_rt_output %>%
  filter(Date >= Sys.Date() - 14) %>%
  ggplot(., aes(Date, Rt)) +
  geom_ribbon(aes(ymin = lower, ymax = upper), fill = "gray80") +
  geom_hline(yintercept = 1, linetype = 'dashed', color = 'blue', size = 1) +
  geom_point(color = "black", size = 2) +
  labs(title = bquote('TMC' ~ R[t] ~ 'Estimate (Past 2 Weeks)')) +
  scale_x_date(date_breaks = '1 day', date_labels = '%m/%d') +
  theme_pubr() +
  theme(axis.text.x = element_text(angle = -45))

rt_cases_stacked = ggarrange(stacked_plot,
                             ggarrange(recent_rt, TMC_plot, ncol = 2),
                             nrow = 2)


# output --------------------------------------------------------------------------------------
fwrite(TMC_rt_output, glue('special-requests/TMC/rt_estimate.csv'), eol = "\r\n")
ggsave('special-requests/TMC/county_plot.png', plot = county_plot, width = 12, height = 8)
ggsave('special-requests/TMC/stacked_plot.png', plot = rt_cases_stacked, width = 10, height = 12, dpi = 600)
