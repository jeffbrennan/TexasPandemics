source("src/rt/common.r")

require("tidyverse")
require("arrow")
require("data.table")
require("furrr")
require("future")
select = dplyr::select


N_CORES = parallel::detectCores()
args = commandArgs(trailingOnly = TRUE)
input_file = args[1]
output_file = args[2]
#  --------------------------------------------------------------------------------------------
county_vitals_raw = arrow::read_parquet(input_file)
county_vitals = county_vitals_raw %>%
  select(County, Date, cases_daily) %>%
  distinct() %>%
  rename(Cases_Daily = cases_daily)

# TODO: update with new pop file (like how it was generated in covid_redux project)
# TODO: put county pops & metadata in tableau level parquet file
population_lookup = unique(fread('https://raw.githubusercontent.com/jeffbrennan/COVID-19/d03d476f7fb060dfd2e1a600a6a1e449df0ab8df/original-sources/DSHS_county_cases.csv')[, c('County', 'Population')])
colnames(population_lookup) = c('Level', 'Population_DSHS')

TMC_COUNTIES = c(
  'Austin', 'Brazoria', 'Chambers', 'Fort Bend', 'Galveston',
  'Harris', 'Liberty', 'Montgomery', 'Waller'
)

daily_df = data.frame(
  Date = seq(
    as.Date('2020-03-15'),
    max(county_vitals$Date), by = 1)
)

#  --------------------------------------------------------------------------------------------
county_vitals_clean = county_vitals %>%
  select(County, Date, Cases_Daily) %>%
  rename(Level = County) %>%
  mutate(Level_Type = 'County') %>%
  left_join(population_lookup, by = 'Level') %>%
  mutate(Population_DSHS = as.integer(Population_DSHS))

rt_prep_combined = county_vitals_clean %>%
  select(Level_Type, Level, Date, Cases_Daily, Population_DSHS) %>%
  rbind(
    county_vitals_clean %>%
      filter(Level %in% TMC_COUNTIES) %>%
      group_by(Date) %>%
      summarize(Cases_Daily = sum(Cases_Daily), Population_DSHS = sum(Population_DSHS)) %>%
      mutate(Level = 'TMC') %>%
      mutate(Level_Type = 'TMC')
  )

rt_prep_df = Prepare_RT(rt_prep_combined)
#  --------------------------------------------------------------------------------------------
start_time = Sys.time()
df_levels = names(rt_prep_df)

message(glue('Running RT on {length(df_levels)} levels using {N_CORES /2} cores'))
rt_start_time = Sys.time()
plan(multisession, workers = (N_CORES / 2), gc = TRUE)

rt_output = future_map(
  rt_prep_df,
  ~Calculate_RT(case_df = .),
  .options = furrr_options(
    seed = 42,
  ),
  .progress = TRUE
)

rt_end_time = Sys.time()
print(rt_end_time - rt_start_time)
plan(sequential)

# formatting --------------------------------------------------------------------------------------------
rt_parsed = map(names(rt_output), ~Parse_RT_Results(., rt_output)) %>%
  rbindlist(fill = TRUE)

# result printout to console
rt_parsed %>%
  group_by(result_success, Level) %>%
  summarize(
    result_success = max(result_success),
    max_date = max(Date)
  ) %>%
  ungroup() %>%
  arrange(max_date) %>%
  mutate(update_delta = Sys.Date() - max_date)

stopifnot(rt_parsed %>%
            filter(Level == 'Bexar') %>%
            filter(is.na(Rt)) %>%
            nrow() < 10)

#   --------------------------------------------------------------------------------------------
rt_df_out = rt_parsed %>%
  filter(Date != max(Date)) %>%
  select(Level_Type, Level, Date, Rt, lower, upper, result_success) %>%
  arrange(Level_Type, Level, Date)


check_rt_combined_dupe = rt_df_out %>%
  group_by(Level_Type, Level, Date) %>%
  filter(n() > 1) %>%
  nrow() == 0

# diagnostics --------------------------------------------------------------------------------------------
# all separate
Visual_Diagnostics = function(df, target_level) {
  all_rt_plot = df %>%
    ggplot(aes(x = Date, y = Rt)) +
    geom_line() +
    geom_point() +
    facet_wrap(~Level, scales = 'free_y') +
    theme_bw()

  # together
  stacked_rt_plot = rt_df_out %>%
    ggplot(aes(x = Date, y = Rt, color = Level)) +
    geom_line() +
    geom_point() +
    theme_bw()

  # together with one highlight
  highlight_plot = rt_df_out %>%
    mutate(Highlight = ifelse(Level == target_level, target_level, 'Other')) %>%
    mutate(Highlight = factor(Highlight, levels = c(target_level, 'Other'))) %>%
    ggplot(aes(x = Date, y = Rt, color = Highlight, alpha = Highlight)) +
    geom_line() +
    scale_color_manual(values = c('red', 'black')) +
    scale_alpha_manual(values = c(1, 0.1)) +
    theme_bw()

  output = list(
    all_rt_plot = all_rt_plot,
    stacked_rt_plot = stacked_rt_plot,
    highlight_plot = highlight_plot
  )
  return(output)
}

# diagnostics = Visual_Diagnostics(rt_df_out, 'Harris')
# diagnostics$highlight_plot

# checks --------------------------------------------------------------------------------------------
arrow::write_parquet(rt_df_out, output_file)
print("Finished writing")