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

select = dplyr::select
set.seed(1)


# pull data --------------------------------------------------------------------------------------------
Clean_Data = function(df, level_type) {
  if (level_type == 'State') {
    clean_df = df %>%
      dplyr::select(Date, Cases_Daily, Tests_Daily, Population_DSHS) %>%
      group_by(Date) %>%
      mutate_if(is.numeric, sum, na.rm = TRUE) %>%
      distinct() %>%
      mutate(Date = as.Date(Date)) %>%
      arrange(Date) %>%
      distinct()

  } else {
    clean_df = df %>%
      dplyr::select(Date, !!as.name(level_type), Cases_Daily, Tests_Daily, Population_DSHS) %>%
      group_by(Date, !!as.name(level_type)) %>%
      mutate_if(is.numeric, sum, na.rm = TRUE) %>%
      distinct() %>%
      mutate(Date = as.Date(Date)) %>%
      arrange(Date, !!as.name(level_type)) %>%
      distinct()
  }
  return(clean_df %>% ungroup())
}

# county_tpr = read.csv('tableau/county_TPR.csv') %>%
#   dplyr::select(County, Date) %>%
#   mutate(Date = as.Date(Date))

county = read.csv("tableau/county.csv") %>%
  dplyr::select(Date, Cases_Daily, Tests_Daily,
                County, TSA_Combined, PHR_Combined,
                Metro_Area, Population_DSHS) %>%
  mutate(Date = as.Date(Date)) %>%
  # left_join(., county_tpr, by = c('County', 'Date')) %>%
  # mutate(TPR = ifelse(Date < min(county_tpr$Date), NA, TPR),
  # Cases_100K_7Day_MA = ifelse(Date < min(county_tpr$Date), NA, Cases_100K_7Day_MA)) %>%
  # group_by(County) %>%
  # tidyr::fill(TPR, .direction = "down") %>%
  # tidyr::fill(Cases_100K_7Day_MA, .direction = "down") %>%
  # ungroup(County) %>%
  rename(TSA = TSA_Combined, PHR = PHR_Combined, Metro = Metro_Area)

clean_dfs = sapply(c('County', 'TSA', 'PHR', 'Metro', 'State'), function(x) Clean_Data(county, x))

# add hospitalizations
hospitalizations = read.csv("tableau/hospitalizations_tsa.csv") %>%
  dplyr::select(Date, TSA_Combined, Hospitalizations_Total) %>%
  rename(TSA = TSA_Combined) %>%
  mutate(Date = as.Date(Date)) %>%
  arrange(Date, TSA)

# hospitalizations %>% View()


clean_dfs$TSA = clean_dfs$TSA %>% left_join(., hospitalizations, by = c('Date', 'TSA'))

state_hosp = hospitalizations %>%
  group_by(Date) %>%
  summarize(Hospitalizations_Total = sum(Hospitalizations_Total))

clean_dfs$State = clean_dfs$State %>% left_join(., state_hosp, by = 'Date')

County_df = clean_dfs$County
TSA_df    = clean_dfs$TSA |> distinct()
PHR_df    = clean_dfs$PHR
Metro_df  = clean_dfs$Metro
State_df  = clean_dfs$State


case_quant = County_df %>%
  filter(Date >= (max(Date) - as.difftime(3, unit = 'weeks'))) %>%
  group_by(County) %>%
  mutate(mean_cases = mean(Cases_Daily, na.rm = TRUE)) %>%
  dplyr::select(mean_cases) %>%
  ungroup() %>%
  summarize(case_quant = quantile(mean_cases, c(0.4, 0.5, 0.6, 0.7, 0.8), na.rm = TRUE)[4]) %>%
  unlist()


# rt analysis --------------------------------------------------------------------------------------------
rt.df.extraction = function(Rt.estimate.output) {

  # extract r0 estimate values into dataframe
  rt.df      = setNames(stack(Rt.estimate.output$estimates$TD$R)[2:1], c('Date', 'Rt'))
  rt.df$Date = as.Date(rt.df$Date)

  # get 95% CI
  CI.lower.list = Rt.estimate.output$estimates$TD$conf.int$lower
  CI.upper.list = Rt.estimate.output$estimates$TD$conf.int$upper

  #use unlist function to format as vector
  CI.lower = unlist(CI.lower.list, recursive = TRUE, use.names = TRUE)
  CI.upper = unlist(CI.upper.list, recursive = TRUE, use.names = TRUE)

  rt.df$lower = CI.lower
  rt.df$upper = CI.upper

  rt.df = rt.df %>%
    mutate(lower = replace(lower, Rt == 0, NA)) %>%
    mutate(upper = replace(upper, Rt == 0, NA)) %>%
    mutate(Rt = replace(Rt, Rt == 0, NA))

  return(rt.df)
}

covid.rt = function(mydata, threshold) {
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
    filter(Date > seq(max(Date), length = 2, by = "-3 weeks")[2]) %>%
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


  #get row number of March 15 and first nonzero entry
  #NOTE: for 7 day moving average start March 15, for daily start March 9
  #find max row between the two (this will be beginning of rt data used)
  march15.row   = which(mydata$Date == "2020-03-15")
  first.nonzero = min(which(mydata$Cases_Daily > 0))
  last.nonzero  = max(which(mydata$Cases_Daily > 0))

  first.nonzero = ifelse(is.infinite(first.nonzero), NA, first.nonzero)
  last.nonzero  = ifelse(is.infinite(last.nonzero), NA, last.nonzero)

  minrow = max(march15.row, first.nonzero, na.rm = TRUE)
  maxrow = as.integer(min(last.nonzero, nrow(mydata), na.rm = TRUE))

  # restrict df to same region as the minrow for addition of TPR & Cases/100
  # TODO: work entirely off mydata and remove individual vars for which row is where
  mydata = mydata %>% slice(minrow:maxrow)

  ### R0 ESTIMATION ###
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

    rt.DSHS.df <<- rt.df.extraction(rt.DSHS) %>%
      dplyr::select(Date, Rt, lower, upper)

    rt.DSHS.df$case_avg  = recent_case_avg
    rt.DSHS.df$threshold = ifelse(recent_case_avg > threshold, 'Above', 'Below')

  },
    error = function(e) {
      writeLines(paste0('Rt generation error (despite sufficient cases)', '\n'))
      rt.DSHS.df <<- data.frame(Date      = as.Date(mydata$Date),
                                Rt        = rep(NA, length(mydata$Date)),
                                lower     = rep(NA, length(mydata$Date)),
                                upper     = rep(NA, length(mydata$Date)),
                                case_avg  = NA,
                                threshold = NA)
    })
  return(rt.DSHS.df)
}

start_time       = Sys.time()
# 13.7 minutes
county.rt.output = nlme::gapply(County_df, FUN = covid.rt,
                                groups         = County_df$County, threshold = case_quant)
print(Sys.time() - start_time)

# combine list of dataframes (1 for each county) to single dataframe
RT_County_df_all = rbindlist(county.rt.output, idcol = 'County') %>%
  mutate(Date = as.Date(Date))

RT_County_df_all$County %>% unique() %>% length()

# remove errors
min_date       = seq(max(RT_County_df_all$Date), length = 2, by = "-3 weeks")[2]
error_counties = RT_County_df_all %>%
  group_by(County) %>%
  mutate(CI_error = factor(ifelse(lower == 0 & upper == 0, 1, 0))) %>%
  mutate(Rt_error = factor(ifelse(is.na(Rt) | Rt == 0 | Rt > 10, 1, 0))) %>%
  filter(Date > min_date & Date != max(Date)) %>%
  filter(is.na(CI_error) | CI_error == 1 | Rt_error == 1) %>%
  dplyr::select(County) %>%
  distinct() %>%
  unlist()


RT_County_df  = RT_County_df_all %>%
  mutate(Rt = ifelse(County %in% error_counties, NA, Rt)) %>%
  mutate(lower = ifelse(County %in% error_counties, NA, lower)) %>%
  mutate(upper = ifelse(County %in% error_counties, NA, upper))
good_counties = RT_County_df$County %>% unique() %>% length()
good_counties / 254 # = 0.744

district = readxl::read_xlsx('tableau/district_school_reopening.xlsx', sheet = 1) %>%
  mutate(LEA  = as.character(LEA),
         Date = as.Date(Date))

district_dates =
  data.frame('Date'   = rep(unique(district$Date), each = 254),
             'County' = rep(unique(County_df$County), times = length(unique(district$Date))))

TPR_df = read.csv('tableau/county_TPR.csv') %>%
  dplyr::select(-contains('Rt')) %>%
  mutate(Date = as.Date(Date))

cms_dates = list.files('C:/Users/jeffb/Desktop/Life/personal-projects/COVID/original-sources/historical/cms_tpr') %>%
  gsub('TPR_', '', .) %>%
  gsub('.csv', '', .) %>%
  as.Date()


cms_TPR_padded =
  TPR_df %>%
    filter(Date %in% cms_dates) %>%
    left_join(., RT_County_df[, c('County', 'Date', 'Rt')], by = c('County', 'Date')) %>%
    full_join(., district_dates, by = c('County', 'Date')) %>%
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
  left_join(., RT_County_df[, c('County', 'Date', 'Rt')], by = c('County', 'Date'))

county_TPR    = cms_TPR_padded %>%
  filter(!(Date %in% district_dates$Date)) %>%
  rbind(cpr_TPR) %>%
  arrange(County, Date)
county_TPR_sd = cms_TPR_padded %>%
  filter(Date %in% district_dates$Date) %>%
  rbind(cpr_TPR) %>%
  arrange(County, Date)

write.csv(county_TPR, 'tableau/county_TPR.csv', row.names = FALSE)
write.csv(county_TPR_sd, 'tableau/county_TPR_sd.csv', row.names = FALSE)

RT_TSA_output = nlme::gapply(TSA_df, FUN = covid.rt, groups = TSA_df$TSA, threshold = case_quant)
RT_TSA_df     = rbindlist(RT_TSA_output, idcol = 'TSA')

RT_PHR_output = nlme::gapply(PHR_df, FUN = covid.rt, groups = PHR_df$PHR, threshold = case_quant)
RT_PHR_df     = rbindlist(RT_PHR_output, idcol = 'PHR')

RT_Metro_output = nlme::gapply(Metro_df, FUN = covid.rt, groups = Metro_df$Metro, threshold = case_quant)
RT_Metro_df     = rbindlist(RT_Metro_output, idcol = 'Metro')

RT_State_df = covid.rt(State_df, threshold = case_quant)

colnames(RT_County_df)[1] = 'Level'
colnames(RT_Metro_df)[1]  = 'Level'
colnames(RT_TSA_df)[1]    = 'Level'
colnames(RT_PHR_df)[1]    = 'Level'
RT_State_df$Level         = 'Texas'

RT_County_df$Level_Type = 'County'
# RT_District_df$Level_Type = 'District'
RT_Metro_df$Level_Type  = 'Metro'
RT_TSA_df$Level_Type    = 'TSA'
RT_PHR_df$Level_Type    = 'PHR'
RT_State_df$Level_Type  = 'State'

RT_Combined_df =
  rbind(RT_County_df, RT_TSA_df, RT_PHR_df, RT_Metro_df, RT_State_df) %>%
    filter(Date != max(Date)) %>%
    dplyr::select(-c(threshold, case_avg))

write.csv(RT_Combined_df, 'tableau/stacked_rt.csv', row.names = FALSE)

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
                                 FUN = covid.arima.forecast,
                                 groups = PHR_df$PHR,
                                 threshold = case_quant)

ARIMA_Case_PHR_df = rbindlist(ARIMA_Case_PHR_output, idcol='PHR')

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
