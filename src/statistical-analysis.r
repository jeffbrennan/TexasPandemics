# setup --------------------------------------------------------------------------------------------
library(tidyverse)
library(reshape2)
library(nlme)        # gapply
library(data.table)

# Modeling & stats
library(mgcv)       # gam
library(R0)         # rt
library(Kendall)    # Mann-Kendall

# Time series & forecasting
library(forecast)
library(zoo)
library(astsa)
library(fpp2)
library(pracma)

library(furrr)
library(future)
library(lubridate)
library(glue)

select = dplyr::select
set.seed(1)

N_CORES         = availableCores()
GENERATION_TIME = generation.time("gamma", c(3.96, 4.75))


# pull data --------------------------------------------------------------------------------------------
Parse_RT_Results = function(level_combined, rt_results_raw) {
  rt_results_level = rt_results_raw[[level_combined]]
  case_df          = rt_prep_df[[level_combined]]

  level_parsed = strsplit(level_combined, split = ';')[[1]]
  level        = level_parsed[1]
  case_type    = level_parsed[2]
  level_type   = case_df$Level_Type[1]

  if (all(is.na(rt_results_level))) {
    # message(glue('{level}: Rt generation error (despite sufficient cases)'))

    result_df = data.frame(Date       = as.Date(case_df$Date),
                           Case_Type  = case_type,
                           Level_Type = level_type,
                           Level      = level,
                           Rt         = rep(NA, length(case_df$Date)),
                           lower      = rep(NA, length(case_df$Date)),
                           upper      = rep(NA, length(case_df$Date)),
                           case_avg   = NA,
                           threshold  = NA)

  } else {
    rt_results = rt_results_level$estimates$TD

    # extract r0 estimate values into dataframe
    result_df = data.frame('Rt' = rt_results[['R']]) %>%
      mutate(
        Case_Type  = case_type,
        Level_Type = level_type,
        Level      = level,
        case_avg   = case_df$case_avg[1],
        threshold  = case_df$threshold[1]
      ) %>%
      mutate(Date = as.Date(row.names(.))) %>%
      as.data.frame(row.names = 1:nrow(.)) %>%
      mutate(lower = rt_results[['conf.int']][['lower']]) %>%
      mutate(upper = rt_results[['conf.int']][['upper']]) %>%
      rowwise() %>%
      mutate(across(c(Rt, lower, upper), ~ifelse(Rt == 0, NA, .))) %>%
      ungroup() %>%
      select(Date, Level_Type, Level, Case_Type, Rt, lower, upper, case_avg, threshold)
  }
  return(result_df)
}

Calculate_RT = function(case_df) {
  # message(level)
  set.seed(1)
  level     = case_df$Level[1]
  level_pop = population_lookup %>%
    filter(Level == level) %>%
    pull(Population_DSHS)

  cases_ma7 = case_df %>%
    select(Date, MA_7day) %>%
    deframe()

  # TODO: add better error handling
  rt_raw = tryCatch(
  {
    result = suppressWarnings(
      estimate.R(
        epid     = cases_ma7,
        GT       = GENERATION_TIME,
        begin    = 1L,
        end      = length(cases_ma7),
        methods  = 'TD',
        pop.size = level_pop,
        nsim     = 1000
      )
    )
    return(result)
  },
    error = function(e) {
      return(NA)
    }
  )
  return(rt_raw)
}

MIN_CONFIRMED_DATE      = as.Date('2020-03-08')
MIN_CONFIRMED_PROB_DATE = as.Date('2022-04-01')

Prepare_RT = function(case_df) {
  # get case average from past month
  recent_case_avg_df = case_df %>%
    group_by(Level_Type, Level, Case_Type) %>%
    filter(Date > max(Date, na.rm = TRUE) - weeks(3)) %>%
    summarize(recent_case_avg = mean(Cases_Daily, na.rm = TRUE)) %>%
    ungroup() %>%
    select(Level_Type, Level, Case_Type, recent_case_avg)

  case_df_final = case_df %>%
    left_join(recent_case_avg_df, by = c('Level_Type', 'Level', 'Case_Type')) %>%
    group_by(Case_Type, Level_Type, Level) %>%
    mutate(MA_7day = rollmean(Cases_Daily, k = 7, na.pad = TRUE, align = 'right')) %>%
    mutate(
      keep_row = ifelse(Case_Type == 'confirmed',
                        Date >= (MIN_CONFIRMED_DATE + days(7)) & Cases_Daily > 0,
                        Date >= (MIN_CONFIRMED_PROB_DATE + days(7)) & Cases_Daily > 0)
    ) %>%
    mutate(keep_row = ifelse(keep_row, TRUE, NA)) %>%
    fill(keep_row, .direction = 'down') %>%
    filter(keep_row) %>%
    slice(1:max(which(Cases_Daily > 0))) %>%
    ungroup() %>%
    left_join(case_quant, by = 'Case_Type') %>%
    select(Date, Case_Type, Level_Type, Level, Cases_Daily, MA_7day, Population_DSHS, recent_case_avg, case_quant) %>%
    rename(
      case_avg  = recent_case_avg,
      threshold = case_quant
    ) %>%
    group_split(Level, Case_Type) %>%
    set_names(map_chr(., ~str_c(.x$Level[1], ';', .x$Case_Type[1])))
  return(case_df_final)
}

Clean_Data = function(df, level_type) {
  if (level_type == 'State') {
    clean_df = df %>%
      select(Date, Case_Type, Cases_Daily, Tests, Population_DSHS) %>%
      group_by(Case_Type, Date) %>%
      summarize(across(c(Cases_Daily, Tests, Population_DSHS), ~sum(., na.rm = TRUE))) %>%
      ungroup() %>%
      mutate(Date = as.Date(Date)) %>%
      arrange(Date) %>%
      mutate(Level_Type = level_type) %>%
      mutate(Level = 'Texas')

  } else {
    clean_df = df %>%
      select(Date, Case_Type, !!as.name(level_type), Cases_Daily, Tests, Population_DSHS) %>%
      group_by(Case_Type, Date, !!as.name(level_type)) %>%
      summarize(across(c(Cases_Daily, Tests, Population_DSHS), ~sum(., na.rm = TRUE))) %>%
      ungroup() %>%
      mutate(Date = as.Date(Date)) %>%
      arrange(Date, !!as.name(level_type)) %>%
      mutate(Level_Type = level_type) %>%
      rename(Level = !!as.name(level_type))
  }
  return(clean_df)
}

# load --------------------------------------------------------------------------------------------
case_levels = c('County', 'TSA', 'PHR', 'Metro', 'State')

TPR = fread('tableau/county_TPR.csv') %>%
  mutate(Date = as.Date(Date)) %>%
  dplyr::select(Date, County, Tests)

county_metadata = fread('tableau/helpers/county_metadata.csv')

county = fread('tableau/county.csv') %>%
  select(Date, Case_Type, County, Cases_Daily) %>%
  mutate(Date = as.Date(Date)) %>%
  left_join(TPR, by = c('Date', 'County')) %>%
  left_join(
    county_metadata %>%
      select(
        County, TSA_Combined, PHR_Combined, Metro_Area,
        Population_2021_07_01
      ) %>%
      rename(Population_DSHS = Population_2021_07_01),
    by = 'County') %>%
  mutate(Date = as.Date(Date)) %>%
  select(Date, County, TSA_Combined, PHR_Combined, Metro_Area, Case_Type, Cases_Daily, Tests, Population_DSHS) %>%
  rename(TSA = TSA_Combined, PHR = PHR_Combined, Metro = Metro_Area)

# combine --------------------------------------------------------------------------------------------

county_combined = map(case_levels, ~Clean_Data(county, .)) %>%
  rbindlist(., fill = TRUE) %>%
  relocate(Level_Type, .before = 'Level')

cleaned_cases_combined = county_combined %>%
  select(-Tests) %>%
  mutate(Cases_Daily = ifelse(is.na(Cases_Daily) | Cases_Daily < 0,
                              0,
                              Cases_Daily
  )
  )

population_lookup = cleaned_cases_combined %>%
  select(Level, Population_DSHS) %>%
  distinct()

case_quant = cleaned_cases_combined %>%
  filter(Level_Type == 'County') %>%
  filter(Date >= (max(Date) - as.difftime(3, unit = 'weeks'))) %>%
  group_by(Level, Case_Type) %>%
  summarize(mean_cases = mean(Cases_Daily, na.rm = TRUE)) %>%
  group_by(Case_Type) %>%
  summarize(case_quant = quantile(mean_cases, c(0.4, 0.5, 0.6, 0.7, 0.8), na.rm = TRUE)[4])

# perform initial rt preperation to minimize parallelized workload
rt_prep_df = Prepare_RT(cleaned_cases_combined)
# rt loop --------------------------------------------------------------------------------------------
# Generate Rt estimates for each county, using 70% quantile of cases in past 3 weeks as threshold
start_time = Sys.time()
df_levels  = names(rt_prep_df)
message(glue('Running RT on {length(df_levels)} levels using {N_CORES} cores'))
rt_start_time = Sys.time()
plan(multisession, workers = N_CORES, gc = TRUE)
rt_output   = future_map(rt_prep_df,
                         ~Calculate_RT(case_df = .),
                         .options  = furrr_options(seed       = TRUE,
                                                   scheduling = Inf),
                         .progress = TRUE
)
rt_end_time = Sys.time()
print(rt_end_time - rt_start_time)
plan(sequential)

# formatting --------------------------------------------------------------------------------------------
rt_parsed = map(names(rt_output), ~Parse_RT_Results(., rt_output)) %>%
  rbindlist(fill = TRUE)

stopifnot(rt_parsed %>%
            filter(Level == 'Harris') %>%
            filter(is.na(Rt)) %>%
            nrow() < 10)

#  --------------------------------------------------------------------------------------------
RT_County_df = rt_parsed %>%
  filter(Level_Type == 'County') %>%
  rename(County = Level)
TPR_df       = read.csv('tableau/county_TPR.csv') %>%
  select(-any_of('Rt')) %>%
  mutate(Date = as.Date(Date))

cms_dates = list.files('original-sources/historical/cms_tpr') %>%
  gsub('TPR_', '', .) %>%
  gsub('.csv', '', .) %>%
  as.Date()

cms_TPR_padded =
  TPR_df %>%
    filter(Date %in% cms_dates) %>%
    left_join(., RT_County_df[, c('County', 'Case_Type', 'Date', 'Rt')], by = c('County', 'Date')) %>%
    group_by(County) %>%
    arrange(County, Date) %>%
    tidyr::fill(TPR, .direction = 'up') %>%
    tidyr::fill(Tests, .direction = 'up') %>%
    tidyr::fill(Rt, .direction = 'up') %>%
    arrange(County, Date) %>%
    ungroup() %>%
    mutate(Tests = ifelse(Date < as.Date('2020-09-09'), NA, Tests))

cpr_TPR = TPR_df %>%
  filter(!(Date %in% cms_dates)) %>%
  left_join(., RT_County_df[, c('County', 'Case_Type', 'Date', 'Rt')], by = c('County', 'Date'), multiple = 'all')

county_TPR = cms_TPR_padded %>%
  rbind(cpr_TPR) %>%
  arrange(County, Date)

check_dupe = county_TPR %>%
  group_by(County, Date, Case_Type) %>%
  filter(n() > 1) %>%
  nrow() == 0

stopifnot(check_dupe)
# fwrite(county_TPR, 'tableau/county_TPR.csv')
#   --------------------------------------------------------------------------------------------
rt_df_out = rt_parsed %>%
  filter(Date != max(Date)) %>%
  select(Case_Type, Level_Type, Level, Date, Rt, lower, upper) %>%
  arrange(Case_Type, Level_Type, Level, Date)


check_rt_combined_dupe = rt_df_out %>%
  group_by(Case_Type, Level_Type, Level, Date) %>%
  filter(n() > 1) %>%
  nrow() == 0

stopifnot(check_rt_combined_dupe)

fwrite(rt_df_out, 'tableau/stacked_rt.csv')
# timeseries --------------------------------------------------------------------------------------------
# Compute forecast (UPDATE PREDICTION PERIOD [days] AS NEEDED)
Predict_Cases = function(df, prediction.period = 10) {
  mindate = min(df$Date)
  maxdate = max(df$Date)

  # print(as.character(mydata[1, 2]))
  # pred_start_label = format(mindate, format = '%m_%d')
  #
  # mydata       = subset(mydata, Date >= mindate)
  prediction.period = 10L
  model.length      = nrow(df) + prediction.period
  recent_case_avg   = df$case_avg[1]
  threshold         = df$threshold[1]

  if (recent_case_avg >= threshold) {
    my.timeseries = ts(df$Cases_Daily)
    my.timeseries = movavg(my.timeseries, 7, "s")

    arima.fit      = forecast::auto.arima(my.timeseries)
    arima.forecast = forecast::forecast(arima.fit, h = prediction.period, level = c(95, 95))

    #return a dataframe of the arima model(Daily cases by date)
    arima.out = data.frame(
      Date        = seq(mindate, (maxdate + prediction.period), by = 'days'),
      Case_Type   = df$Case_Type[1],
      Level_Type  = df$Level_Type[1],
      Level       = df$Level[1],
      Cases_Raw   = c(df$Cases_Daily, rep(NA, times = prediction.period)),
      Cases_Daily = c(my.timeseries, arima.forecast[['mean']]),
      CI_Lower    = c(rep(NA, times = length(my.timeseries)),
                      arima.forecast[['lower']][, 2]),
      CI_Upper    = c(rep(NA, times = length(my.timeseries)),
                      arima.forecast[['upper']][, 2])) %>%
      mutate(CI_Lower = ifelse(CI_Lower <= 0, 0, CI_Lower))

  } else {
    # insufficient data catch: return NA values for predictions
    arima.out = data.frame(
      Date        = seq(mindate, maxdate + prediction.period, by = 'days'),
      Case_Type   = df$Case_Type[1],
      Level_Type  = df$Level_Type[1],
      Level       = df$Level[1],
      Cases_Raw   = c(df$Cases_Daily, rep(NA, times = prediction.period)),
      Cases_Daily = rep(NA, times = model.length),
      CI_Lower    = rep(NA, times = model.length),
      CI_Upper    = rep(NA, times = model.length))
  }
  return(arima.out)
}


# run arima --------------------------------------------------------------------------------------------

plan(multisession, workers = N_CORES, gc = FALSE)
arima_case_start_time = Sys.time()
arima_case_output     = future_map(rt_prep_df,
                                   ~Predict_Cases(df = .),
                                   .options  = furrr_options(seed       = TRUE,
                                                             scheduling = Inf),
                                   .progress = TRUE
)
print(Sys.time() - arima_case_start_time)
plan(sequential)
# combine --------------------------------------------------------------------------------------------
ARIMA_Case_Combined_df = rbindlist(arima_case_output)

# upload --------------------------------------------------------------------------------------------
stopifnot(max(ARIMA_Case_Combined_df$Date) > Sys.Date())
fwrite(ARIMA_Case_Combined_df, 'tableau/stacked_case_timeseries.csv')

# hosp time series --------------------------------------------------------------------------------------------

Predict_Hospitalizations = function(mydata, prediction.period = 10) {
  mindate = as.Date('2020-04-12')
  maxdate = max(mydata$Date)
  mydata  = mydata %>% filter(Date >= mindate)

  model.length = nrow(mydata) + prediction.period

  if (max(mydata$Hospitalizations_Total >= 100, na.rm = TRUE))
  {
    my.timeseries = ts(mydata$Hospitalizations_Total)
    my.timeseries = movavg(my.timeseries, 7, "s")

    arima.fit      = forecast::auto.arima(my.timeseries)
    arima.forecast = forecast::forecast(arima.fit, h = prediction.period, level = c(95, 95))

    #return a dataframe of the arima model (Daily cases by date)
    arima.out = data.frame(
      Date                   = seq(mindate, maxdate + prediction.period, by = 'days'),
      Level_Type             = mydata$Level_Type[1],
      Level                  = mydata$Level[1],
      Hospitalizations_Total = c(my.timeseries, arima.forecast[['mean']]),
      CI_Lower               = c(rep(NA, times = length(my.timeseries)),
                                 arima.forecast[['lower']][, 2]),
      CI_Upper               = c(rep(NA, times = length(my.timeseries)),
                                 arima.forecast[['upper']][, 2])) %>%
      mutate(CI_Lower = ifelse(CI_Lower <= 0, 0, CI_Lower))
  } else {
    # insufficient data catch: return NA values for predictions
    arima.out = data.frame(
      Date                   = seq(mindate, maxdate + prediction.period, by = 'days'),
      Level_Type             = mydata$Level_Type[1],
      Level                  = mydata$Level[1],
      Hospitalizations_Total = rep(NA, times = model.length),
      CI_Lower               = rep(NA, times = model.length),
      CI_Upper               = rep(NA, times = model.length))
  }
  return(arima.out)
}

# hosp setup --------------------------------------------------------------------------------------------
hospitalizations = fread("tableau/hospitalizations_tsa.csv") %>%
  select(Date, TSA_Combined, Hospitalizations_Total) %>%
  rename(TSA = TSA_Combined) %>%
  mutate(Date = as.Date(Date)) %>%
  arrange(Date, TSA)

hospitalizations_df_split = hospitalizations %>%
  rename(Level = TSA) %>%
  mutate(Level_Type = 'TSA') %>%
  rbind(
    hospitalizations %>%
      group_by(Date) %>%
      summarize(Hospitalizations_Total = sum(Hospitalizations_Total, na.rm = TRUE)) %>%
      mutate(Level = 'Texas') %>%
      mutate(Level_Type = 'State')
  ) %>%
  group_split(Level) %>%
  set_names(map_chr(., ~str_c(.x$Level[1])))

plan(multisession, workers = N_CORES, gc = FALSE)
arima_hosp_start_time  = Sys.time()
ARIMA_Hosp_Combined_df = future_map(hospitalizations_df_split, ~Predict_Hospitalizations(.)) %>%
  rbindlist() %>%
  mutate(Date = as.Date(Date))
print(Sys.time() - arima_hosp_start_time)

plan(sequential)
fwrite(ARIMA_Hosp_Combined_df, 'tableau/stacked_hosp_timeseries.csv')

# standard stats --------------------------------------------------------------------------------------------
# pct change --------------------------------------------------------------------------------------------
new_pct_change = function(df) {
  # creates the % difference in cases and tests and smooth line with CIs
  # level: either "TSA", "County", or "Metro". Note that "county" won't work for many counties unless have enough cases.
  # dat: dataset (e.g. "county", "metro", "tsa")
  # region: the region within the dataset (county, metro region, or tsa)
  # restrict data to first test date (for % test increase)

  start_values = df %>% filter(Date == min(Date))

  result_df = df %>%
    mutate(
      cases_ma_percentdiff    = 100 * ((Cases_MA_14 / start_values$Cases_MA_14) - 1),
      cases_total_percentdiff = 100 * ((Cases_Total_14 / start_values$Cases_Total_14) - 1),
      tests_ma_percentdiff    = 100 * ((Tests_MA_14 / start_values$Tests_MA_14) - 1),
      tests_total_percentdiff = 100 * ((Tests_Total_14 / start_values$Tests_Total_14) - 1)
    ) %>%
    rename(cases_ma       = Cases_MA_14,
           cases_total_14 = Cases_Total_14,
           tests_ma       = Tests_MA_14,
           tests_total_14 = Tests_Total_14
    ) %>%
    select(Date, Level_Type, Level, Case_Type,
           cases_ma, cases_total_14, tests_ma, tests_total_14,
           cases_ma_percentdiff, cases_total_percentdiff, tests_ma_percentdiff, tests_total_percentdiff)


  return(result_df)
}


TPR_dates = list.files('original-sources/historical/cms_tpr/') %>%
  gsub('TPR_', '', .) %>%
  gsub('.csv', '', .) %>%
  as.Date(.)

PCT_START_DATE = as.Date("2020-09-30")

PCT_prep_df = county_combined %>%
  select(Date, Case_Type, Level_Type, Level, Cases_Daily, Tests) %>%
  group_by(Case_Type, Level_Type, Level) %>%
  arrange(Date) %>%
  mutate(Tests = ifelse(Tests == 0, NA, Tests)) %>%
  tidyr::fill(Tests, .direction = 'down') %>%
  mutate(Cases_Total_14 = rollsum(Cases_Daily, k = 14, na.pad = TRUE, align = 'right')) %>%
  mutate(Cases_MA_14 = rollmean(Cases_Daily, k = 14, na.pad = TRUE, align = 'right')) %>%
  mutate(Tests_MA_14 = Tests / 14) %>%
  ungroup() %>%
  filter(Date >= PCT_START_DATE) %>%
  dplyr::select(-Cases_Daily) %>%
  rename(Tests_Total_14 = Tests) %>%
  group_split(Level, Case_Type) %>%
  set_names(map_chr(., ~str_c(.x$Level[1], ';', .x$Case_Type[1])))

# calcs --------------------------------------------------------------------------------------------
pct_start_time = Sys.time()
plan(multisession, workers = N_CORES, gc = TRUE)
PCT_Combined_df = future_map(PCT_prep_df, ~new_pct_change(.x)) %>%
  rbindlist() %>%
  mutate(Date = as.Date(Date)) %>%
  mutate(tests_ma_percentdiff = ifelse(Date > min(Date) & tests_ma_percentdiff == -100, NA, tests_ma_percentdiff)) %>%
  mutate(tests_total_percentdiff = ifelse(Date > min(Date) & tests_total_percentdiff == -100, NA, tests_total_percentdiff))
print(Sys.time() - pct_start_time)
plan(sequential)

fwrite(PCT_Combined_df, 'tableau/stacked_pct_change_new.csv')

# stack combo --------------------------------------------------------------------------------------------

Ratio_Combined_df$Date                = max(PCT_Combined_df$Date)
colnames(ARIMA_Case_Combined_df)[5:6] = c('TS_CI_Lower', 'TS_CI_Upper')
colnames(RT_Combined_df)[4:5]         = c('RT_CI_Lower', 'RT_CI_Upper')
colnames(ARIMA_Hosp_Combined_df)[4:5] = c('TS_Hosp_CI_Lower', 'TS_Hosp_CI_Upper')

stacked_all = Reduce(function(x, y) merge(x, y, by = c('Level_Type', 'Level', 'Date'), all = TRUE),
                     list(PCT_Combined_df, Ratio_Combined_df,
                          ARIMA_Case_Combined_df, RT_Combined_df, ARIMA_Hosp_Combined_df))

write.csv(stacked_all, 'tableau/stacked_critical_trends.csv', row.names = FALSE)
