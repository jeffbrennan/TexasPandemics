require("R0")    # rt calculation
require("zoo")   # moving averages
require("glue")  # message output
require("lubridate")
select = dplyr::select
GENERATION_TIME = generation.time("gamma", c(3.96, 4.75))


# pull data --------------------------------------------------------------------------------------------
Parse_RT_Results = function(level_combined, rt_results_raw) {
  rt_results_level = rt_results_raw[[level_combined]]
  case_df = rt_prep_df[[level_combined]]
  threshold = 0
  level = case_df$Level[1]
  level_type = case_df$Level_Type[1]

  if (all(is.na(rt_results_level))) {
    # message(glue('{level}: Rt generation error (despite sufficient cases)'))

    result_df = data.frame(
      Date = as.Date(case_df$Date),
      Level_Type = level_type,
      Level = level,
      Rt = rep(NA, length(case_df$Date)),
      lower = rep(NA, length(case_df$Date)),
      upper = rep(NA, length(case_df$Date))
    )

  } else {
    rt_results = rt_results_level$estimates$TD

    # extract r0 estimate values into dataframe
    result_df = data.frame('Rt' = rt_results[['R']]) %>%
      mutate(
        Level_Type = level_type,
        Level = level,
        case_avg = case_df$case_avg[1],
        threshold = threshold
      ) %>%
      mutate(Date = as.Date(row.names(.))) %>%
      as.data.frame(row.names = 1:nrow(.)) %>%
      mutate(lower = rt_results[['conf.int']][['lower']]) %>%
      mutate(upper = rt_results[['conf.int']][['upper']]) %>%
      rowwise() %>%
      mutate(across(c(Rt, lower, upper), ~ifelse(Rt == 0, NA, .))) %>%
      ungroup() %>%
      select(Date, Level_Type, Level, Rt, lower, upper)
  }
  return(result_df)
}

Calculate_RT = function(case_df) {
  set.seed(1)
  level_pop = case_df$Population_DSHS[1]

  cases_ma7 = case_df %>%
    select(Date, MA_7day) %>%
    deframe()

  # TODO: add better error handling
  rt_raw = tryCatch(
  {
    result = suppressWarnings(
      estimate.R(
        epid = cases_ma7,
        GT = GENERATION_TIME,
        begin = 1L,
        end = length(cases_ma7),
        methods = 'TD',
        pop.size = level_pop,
        nsim = 1000
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

MIN_CONFIRMED_DATE = as.Date('2020-03-08')
MIN_CONFIRMED_PROB_DATE = as.Date('2022-04-01')

Convert_Cases_To_Daily = function(case_df) {
  case_df_split = case_df %>% group_split(Level_Type, Level)
  case_df_daily = map(
    case_df_split,
    function(x) daily_df %>%
      left_join(x, by = 'Date', multiple = 'all') %>%
      mutate(Level_Type = unique(x$Level_Type)) %>%
      mutate(Level = unique(x$Level)) %>%
      mutate(Cases_Daily = ifelse(is.na(Cases_Daily), 0, Cases_Daily)) %>%
      mutate(Population_DSHS = max(Population_DSHS, na.rm = TRUE)) %>%
      select(Date, Level_Type, Level, Cases_Daily, Population_DSHS)
  ) %>%
    bind_rows()
}

Prepare_RT = function(case_df) {
  # get case average from past month
  recent_case_avg_df = case_df %>%
    group_by(Level_Type, Level) %>%
    filter(Date > max(Date, na.rm = TRUE) - lubridate::weeks(3)) %>%
    summarize(recent_case_avg = mean(Cases_Daily, na.rm = TRUE)) %>%
    ungroup() %>%
    select(Level_Type, Level, recent_case_avg)

  case_df_daily = Convert_Cases_To_Daily(case_df)

  case_df_final = case_df_daily %>%
    left_join(recent_case_avg_df, by = c('Level_Type', 'Level')) %>%
    group_by(Level_Type, Level) %>%
    mutate(MA_7day = rollmean(Cases_Daily, k = 7, na.pad = TRUE, align = 'right')) %>%
    mutate(keep_row = Date >= (MIN_CONFIRMED_DATE + days(7)) & Cases_Daily > 0) %>%
    mutate(keep_row = ifelse(keep_row, TRUE, NA)) %>%
    fill(keep_row, .direction = 'down') %>%
    filter(keep_row) %>%
    slice(1:max(which(Cases_Daily > 0))) %>%
    ungroup() %>%
    select(Date, Level_Type, Level, Cases_Daily, MA_7day, Population_DSHS, recent_case_avg) %>%
    rename(
      case_avg = recent_case_avg,
    ) %>%
    group_split(Level) %>%
    set_names(map_chr(., ~.x$Level[1]))

  return(case_df_final)
}

