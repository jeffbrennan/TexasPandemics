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
        Level      = level
      ) %>%
      mutate(Date = as.Date(row.names(.))) %>%
      as.data.frame(row.names = 1:nrow(.)) %>%
      mutate(lower = rt_results[['conf.int']][['lower']]) %>%
      mutate(upper = rt_results[['conf.int']][['upper']]) %>%
      rowwise() %>%
      mutate(across(c(Rt, lower, upper), ~ifelse(Rt == 0, NA, .))) %>%
      ungroup() %>%
      select(Date, Level_Type, Level, Case_Type, Rt, lower, upper)
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
    select(Date, Case_Type, Level_Type, Level, MA_7day, Population_DSHS) %>%
    group_split(Level, Case_Type) %>%
    set_names(map_chr(., ~str_c(.x$Level[1], ';', .x$Case_Type[1])))
  return(case_df_final)
}

Clean_Data = function(df, level_type) {
  if (level_type == 'State') {
    clean_df = df %>%
      select(Date, Case_Type, Cases_Daily, Population_DSHS) %>%
      group_by(Case_Type, Date) %>%
      summarize(across(c(Cases_Daily, Population_DSHS), ~sum(., na.rm = TRUE))) %>%
      ungroup() %>%
      mutate(Date = as.Date(Date)) %>%
      arrange(Date) %>%
      mutate(Level_Type = level_type) %>%
      mutate(Level = 'Texas')

  } else {
    clean_df = df %>%
      select(Date, Case_Type, !!as.name(level_type), Cases_Daily, Population_DSHS) %>%
      group_by(Case_Type, Date, !!as.name(level_type)) %>%
      summarize(across(c(Cases_Daily, Population_DSHS), ~sum(., na.rm = TRUE))) %>%
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
county_raw  = fread('tableau/county.csv') %>%
  select(Date, Case_Type, County, Cases_Daily)

county_metadata = fread('tableau/helpers/county_metadata.csv')

county = county_raw %>%
  left_join(
    county_metadata %>%
      select(
        County, TSA_Combined, PHR_Combined, Metro_Area,
        Population_2021_07_01
      ) %>%
      rename(Population_DSHS = Population_2021_07_01),
    by = 'County') %>%
  mutate(Date = as.Date(Date)) %>%
  select(Date, County, TSA_Combined, PHR_Combined, Metro_Area, Case_Type, Cases_Daily, Population_DSHS) %>%
  rename(TSA = TSA_Combined, PHR = PHR_Combined, Metro = Metro_Area)

# combine --------------------------------------------------------------------------------------------

cleaned_cases_combined = map(case_levels, ~Clean_Data(county, .)) %>%
  rbindlist(., fill = TRUE) %>%
  relocate(Level_Type, .before = 'Level') %>%
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
# rt_prep_df      = rt_prep_df_orig[230:240]

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

#  --------------------------------------------------------------------------------------------
RT_County_df = rt_parsed %>%
  filter(Level_Type == 'County') %>%
  rename(County = Level)
TPR_df = read.csv('tableau/county_TPR.csv') %>%
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
threshold = case_quant
# Compute forecast (UPDATE PREDICTION PERIOD [days] AS NEEDED)
covid.arima.forecast = function(mydata, prediction.period = 10, mindate, threshold) {
  mindate = min(mydata$Date)
  print(as.character(mydata[1, 2]))
  maxdate          = max(mydata$Date)
  pred_start_label = format(mindate, format = '%m_%d')

  mydata       = subset(mydata, Date >= mindate)
  model.length = as.numeric(length(mydata$Date) + prediction.period)

  recent_case_avg = mydata %>%
    filter(Date > seq(max(Date), length = 2, by = "-3 weeks")[2]) %>%
    summarize(mean(Cases_Daily, na.rm = TRUE)) %>%
    unlist()

  print(recent_case_avg)

  if (recent_case_avg >= threshold) {
    # arima requires cases to be a timeseries vector
    my.timeseries = ts(mydata$Cases_Daily)

    library(pracma)
    my.timeseries = movavg(my.timeseries, 7, "s")

    arima.fit = forecast::auto.arima(my.timeseries)
    # save parameters from arima autofit
    p         = arima.fit$arma[1]          # autoregressive order
    q         = arima.fit$arma[2]          # moving average order
    d         = arima.fit$arma[6]           # differencing order from model

    # 10 day forecast, CI for lower and upper has confidence level 95% set by level =c(95,95)
    arima.forecast = forecast::forecast(arima.fit, h = prediction.period, level = c(95, 95))

    #return a dataframe of the arima model(Daily cases by date)
    arima.out = data.frame(Date        = seq(mindate, maxdate + prediction.period, by = 'days'),
                           Cases_Raw   = c(mydata$Cases_Daily, rep(NA, times = prediction.period)),
                           Cases_Daily = c(my.timeseries, arima.forecast[['mean']]),
                           CI_Lower    = c(rep(NA, times = length(my.timeseries)),
                                           arima.forecast[['lower']][, 2]),
                           CI_Upper    = c(rep(NA, times = length(my.timeseries)),
                                           arima.forecast[['upper']][, 2]))

  } else {
    # insufficient data catch: return NA values for predictions
    arima.out = data.frame(Date        = seq(mindate, maxdate + prediction.period, by = 'days'),
                           Cases_Raw   = c(mydata$Cases_Daily, rep(NA, times = prediction.period)),
                           Cases_Daily = rep(NA, times = model.length),
                           CI_Lower    = rep(NA, times = model.length),
                           CI_Upper    = rep(NA, times = model.length))

  }
  #replace CI lower limit with 0 if negative
  arima.out$CI_Lower = ifelse(arima.out$CI_Lower >= 0, arima.out$CI_Lower, 0)
  return(arima.out)
}


ARIMA_Case_County_output = nlme::gapply(County_df,
                                        FUN       = covid.arima.forecast,
                                        groups    = County_df$County,
                                        threshold = case_quant)

ARIMA_Case_County_df = rbindlist(ARIMA_Case_County_output, idcol = 'County')

ARIMA_Case_TSA_output = nlme::gapply(TSA_df,
                                     FUN       = covid.arima.forecast,
                                     groups    = TSA_df$TSA,
                                     threshold = case_quant)

ARIMA_Case_TSA_df = rbindlist(ARIMA_Case_TSA_output, idcol = 'TSA')

ARIMA_Case_TSA_output = nlme::gapply(TSA_df,
                                     FUN       = covid.arima.forecast,
                                     groups    = TSA_df$TSA,
                                     threshold = case_quant)

ARIMA_Case_TSA_df = rbindlist(ARIMA_Case_TSA_output, idcol = 'TSA')

ARIMA_Case_PHR_output = nlme::gapply(PHR_df,
                                     FUN       = covid.arima.forecast,
                                     groups    = PHR_df$PHR,
                                     threshold = case_quant)

ARIMA_Case_PHR_df = rbindlist(ARIMA_Case_PHR_output, idcol = 'PHR')

ARIMA_Case_Metro_output = nlme::gapply(Metro_df,
                                       FUN       = covid.arima.forecast,
                                       groups    = Metro_df$Metro,
                                       threshold = case_quant)

ARIMA_Case_Metro_df = rbindlist(ARIMA_Case_Metro_output, idcol = 'Metro')

ARIMA_Case_State_df = covid.arima.forecast(State_df, mindate = as.Date('2020-03-04'), threshold = case_quant)

colnames(ARIMA_Case_County_df)[1] = 'Level'
colnames(ARIMA_Case_Metro_df)[1]  = 'Level'
colnames(ARIMA_Case_TSA_df)[1]    = 'Level'
ARIMA_Case_State_df$Level         = 'Texas'
colnames(ARIMA_Case_PHR_df)[1]    = 'Level'

ARIMA_Case_County_df$Level_Type = 'County'
ARIMA_Case_Metro_df$Level_Type  = 'Metro'
ARIMA_Case_TSA_df$Level_Type    = 'TSA'
ARIMA_Case_PHR_df$Level_Type    = 'PHR'
ARIMA_Case_State_df$Level_Type  = 'State'

ARIMA_Case_Combined_df = rbind(ARIMA_Case_County_df, ARIMA_Case_TSA_df, ARIMA_Case_PHR_df,
                               ARIMA_Case_Metro_df, ARIMA_Case_State_df)
write.csv(ARIMA_Case_Combined_df, 'tableau/stacked_case_timeseries.csv', row.names = FALSE)


# hosp time series --------------------------------------------------------------------------------------------

covid.arima.forecast = function(mydata, prediction.period = 10, mindate) {
  maxdate          = max(mydata$Date)
  pred_start_label = format(mindate, format = '%m_%d')

  mydata       = subset(mydata, Date >= mindate)
  model.length = as.numeric(length(mydata$Date) + prediction.period)

  if (max(mydata$Hospitalizations_Total >= 100, na.rm = TRUE))
  {
    my.timeseries = ts(mydata$Hospitalizations_Total)

    library(pracma)
    my.timeseries = movavg(my.timeseries, 7, "s")

    arima.fit = forecast::auto.arima(my.timeseries)

    # save parameters from arima autofit
    p = arima.fit$arma[1]          # autoregressive order
    q = arima.fit$arma[2]          # moving average order
    d = arima.fit$arma[6]           # differencing order from model

    # 10 day forecast, CI for lower and upper has confidence level 95% set by level =c(95,95)
    arima.forecast = forecast::forecast(arima.fit, h = prediction.period, level = c(95, 95))

    #return a dataframe of the arima model (Daily cases by date)
    arima.out = data.frame(Date                   = seq(mindate, maxdate + prediction.period, by = 'days'),
                           # Cases_Raw = c(mydata$Hospitalizations_Total, rep(NA, times = prediction.period)),
                           Hospitalizations_Total = c(my.timeseries, arima.forecast[['mean']]),
                           CI_Lower               = c(rep(NA, times = length(my.timeseries)),
                                                      arima.forecast[['lower']][, 2]),
                           CI_Upper               = c(rep(NA, times = length(my.timeseries)),
                                                      arima.forecast[['upper']][, 2]))
  } else {
    # insufficient data catch: return NA values for predictions
    arima.out = data.frame(Date                   = seq(mindate, maxdate + prediction.period, by = 'days'),
                           # Cases_Raw = c(mydata$Hospitalizations_Total, rep(NA, times = prediction.period)),
                           Hospitalizations_Total = rep(NA, times = model.length),
                           CI_Lower               = rep(NA, times = model.length),
                           CI_Upper               = rep(NA, times = model.length))
  }
  #replace CI lower limit with 0 if negative
  arima.out$CI_Lower = ifelse(arima.out$CI_Lower >= 0, arima.out$CI_Lower, 0)
  return(arima.out)
}

ARIMA_Hosp_TSA_output = nlme::gapply(TSA_df,
                                     FUN     = covid.arima.forecast,
                                     groups  = TSA_df$TSA,
                                     mindate = as.Date('2020-04-12'))

ARIMA_Hosp_TSA_df = rbindlist(ARIMA_Hosp_TSA_output, idcol = 'TSA')

ARIMA_Hosp_State_df = covid.arima.forecast(State_df, mindate = as.Date('2020-04-12'))

colnames(ARIMA_Hosp_TSA_df)[1] = 'Level'
ARIMA_Hosp_State_df$Level      = 'Texas'

ARIMA_Hosp_TSA_df$Level_Type   = 'TSA'
ARIMA_Hosp_State_df$Level_Type = 'State'

ARIMA_Hosp_Combined_df = rbind(ARIMA_Hosp_TSA_df, ARIMA_Hosp_State_df)
write.csv(ARIMA_Hosp_Combined_df, 'tableau/stacked_hosp_timeseries.csv', row.names = FALSE)


# standard stats --------------------------------------------------------------------------------------------
# pct change --------------------------------------------------------------------------------------------
new_pct_change = function(level, dat, region) {
  # creates the % difference in cases and tests and smooth line with CIs
  # level: either "TSA", "County", or "Metro". Note that "county" won't work for many counties unless have enough cases.
  # dat: dataset (e.g. "county", "metro", "tsa")
  # region: the region within the dataset (county, metro region, or tsa)

  if (level != 'State') { dat = dat %>% filter(!!as.name(level) == region) }

  # restrict data to first test date (for % test increase)
  start_date   = as.Date("2020-09-30")
  dat          = dat %>% filter(Date >= start_date)
  start_values = dat %>% filter(Date == start_date)

  dat$ma_cases_y    = 100 * (as.vector(dat$Cases_MA_14 / start_values$Cases_MA_14) - 1)
  dat$total_cases_y = 100 * (as.vector(dat$Cases_Total_14 / start_values$Cases_Total_14) - 1)
  dat$ma_tests_y    = 100 * (as.vector(dat$Tests_MA_14 / start_values$Tests_MA_14) - 1)
  dat$total_tests_y = 100 * (as.vector(dat$Tests_Total_14 / start_values$Tests_Total_14) - 1)

  tmp.df = data.frame(Level                   = region,
                      Date                    = dat$Date,
                      cases_ma                = dat$Cases_MA_14,
                      cases_total_14          = dat$Cases_Total_14,
                      tests_ma                = dat$Tests_MA_14,
                      tests_total_14          = dat$Tests_Total_14,
                      cases_ma_percentdiff    = dat$ma_cases_y,
                      cases_total_percentdiff = dat$total_cases_y,
                      tests_ma_percentdiff    = dat$ma_tests_y,
                      tests_total_percentdiff = dat$total_tests_y,
                      Level_Type              = level)
  return(tmp.df)
}

TPR = read.csv('tableau/county_TPR.csv') %>%
  mutate(Date = as.Date(Date)) %>%
  dplyr::select(Date, County, Tests)


TPR_dates = list.files('original-sources/historical/cms_tpr/') %>%
  gsub('TPR_', '', .) %>%
  gsub('.csv', '', .) %>%
  as.Date(.)


TPR_merge = county %>%
  dplyr::select(-Tests_Daily, -Population_DSHS) %>%
  left_join(TPR, by = c('Date', 'County')) %>%
  group_by(County) %>%
  mutate(Cases_Total_14 = rollsum(Cases_Daily, k = 14, na.pad = TRUE, align = 'right')) %>%
  mutate(Cases_MA_14 = rollmean(Cases_Daily, k = 14, na.pad = TRUE, align = 'right')) %>%
  mutate(Tests_MA_14 = Tests / 14) %>%
  ungroup() %>%
  dplyr::select(-Cases_Daily) %>%
  rename(Tests_Total_14 = Tests) %>%
  filter(!is.na(TSA))


TPR_county = TPR_merge %>% dplyr::select(-c(TSA, PHR, Metro))
TPR_TSA    = TPR_merge %>%
  group_by(Date, TSA) %>%
  summarize_if(is.numeric, sum, na.rm = TRUE)
TPR_PHR    = TPR_merge %>%
  group_by(Date, PHR) %>%
  summarize_if(is.numeric, sum, na.rm = TRUE)
TPR_Metro  = TPR_merge %>%
  group_by(Date, Metro) %>%
  summarize_if(is.numeric, sum, na.rm = TRUE)
TPR_State  = TPR_merge %>%
  group_by(Date) %>%
  summarize_if(is.numeric, sum, na.rm = TRUE)


# calcs --------------------------------------------------------------------------------------------
PCT_County_df = rbindlist(lapply(unique(TPR_county$County), function(x) new_pct_change('County', TPR_county, x)))

PCT_TSA_df = rbindlist(lapply(unique(TPR_TSA$TSA), function(x) new_pct_change('TSA', TPR_TSA, x))) %>%
  mutate(tests_ma_percentdiff = ifelse(Date > min(Date) & tests_ma_percentdiff == -100, NA, tests_ma_percentdiff)) %>%
  mutate(tests_total_percentdiff = ifelse(Date > min(Date) & tests_total_percentdiff == -100, NA, tests_total_percentdiff))

PCT_PHR_df = rbindlist(lapply(unique(TPR_PHR$PHR), function(x) new_pct_change('PHR', TPR_PHR, x))) %>%
  mutate(tests_ma_percentdiff = ifelse(Date > min(Date) & tests_ma_percentdiff == -100, NA, tests_ma_percentdiff)) %>%
  mutate(tests_total_percentdiff = ifelse(Date > min(Date) & tests_total_percentdiff == -100, NA, tests_total_percentdiff))


PCT_Metro_df = rbindlist(lapply(unique(TPR_Metro$Metro), function(x) new_pct_change('Metro', TPR_Metro, x))) %>%
  mutate(tests_ma_percentdiff = ifelse(Date > min(Date) & tests_ma_percentdiff == -100, NA, tests_ma_percentdiff)) %>%
  mutate(tests_total_percentdiff = ifelse(Date > min(Date) & tests_total_percentdiff == -100, NA, tests_total_percentdiff))


PCT_State_df = new_pct_change('State', TPR_State, 'Texas') %>%
  mutate(tests_ma_percentdiff = ifelse(Date > min(Date) & tests_ma_percentdiff == -100, NA, tests_ma_percentdiff)) %>%
  mutate(tests_total_percentdiff = ifelse(Date > min(Date) & tests_total_percentdiff == -100, NA, tests_total_percentdiff))

PCT_Combined_df = rbind(PCT_County_df, PCT_TSA_df, PCT_PHR_df, PCT_Metro_df, PCT_State_df)
write.csv(PCT_Combined_df, 'tableau/stacked_pct_change_new.csv', row.names = FALSE)

# stack combo --------------------------------------------------------------------------------------------

Ratio_Combined_df$Date                = max(PCT_Combined_df$Date)
colnames(ARIMA_Case_Combined_df)[5:6] = c('TS_CI_Lower', 'TS_CI_Upper')
colnames(RT_Combined_df)[4:5]         = c('RT_CI_Lower', 'RT_CI_Upper')
colnames(ARIMA_Hosp_Combined_df)[4:5] = c('TS_Hosp_CI_Lower', 'TS_Hosp_CI_Upper')

stacked_all = Reduce(function(x, y) merge(x, y, by = c('Level_Type', 'Level', 'Date'), all = TRUE),
                     list(PCT_Combined_df, Ratio_Combined_df,
                          ARIMA_Case_Combined_df, RT_Combined_df, ARIMA_Hosp_Combined_df))

write.csv(stacked_all, 'tableau/stacked_critical_trends.csv', row.names = FALSE)
