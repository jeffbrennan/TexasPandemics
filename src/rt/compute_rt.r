require("tidyverse")
require("arrow")
require("data.table")

select = dplyr::select

source("src/rt/common.r")

args = commandArgs(trailingOnly = TRUE)
input_file = args[1]
output_file = args[2]

# input_file = 'data/tableau/county_vitals.parquet'
# output_file = "data/intermediate/rt_results.parquet"
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
  'Harris', 'Liberty', 'Montgomery', 'Waller',
  #testing
  "Bexar"
)

daily_df = data.frame(
  Date = seq(
    as.Date('2020-03-15'),
    max(county_vitals$Date), by = 1)
)

#  --------------------------------------------------------------------------------------------
county_vitals_clean = county_vitals %>%
  select(County, Date, Cases_Daily) %>%
  left_join(county_pops, by = 'County') %>%
  mutate(Population_DSHS = as.integer(Population_DSHS))

rt_prep_combined = county_vitals_clean %>%
  mutate(Level_Type = "County") %>%
  rename(Level = County) %>%
  select(Level_Type, Level, Date, Cases_Daily, Population_DSHS) %>%
  rbind(
    county_vitals_clean %>%
      filter(County %in% TMC_COUNTIES) %>%
      group_by(Date) %>%
      summarize(Cases_Daily = sum(Cases_Daily), Population_DSHS = sum(Population_DSHS)) %>%
      mutate(Level = 'TMC') %>%
      mutate(Level_Type = 'TMC')
  )

rt_prep_df = Prepare_RT(rt_prep_combined)
require("furrr")
require("future")


start_time = Sys.time()
df_levels = names(rt_prep_df)
N_CORES = parallel::detectCores()

message(glue('Running RT on {length(df_levels)} levels using {N_CORES /2} cores'))
rt_start_time = Sys.time()
plan(multisession, workers = (N_CORES / 2), gc = TRUE)

rt_output = future_map(
  rt_prep_df,
  ~Calculate_RT(case_df = .),
  .options = furrr_options(
    seed = 42,
    # scheduling = 1,
    # chunk_size = NULL
  ),
  .progress = TRUE
)

rt_end_time = Sys.time()
print(rt_end_time - rt_start_time)
plan(sequential)

# formatting --------------------------------------------------------------------------------------------
rt_parsed = map(names(rt_output), ~Parse_RT_Results(., rt_output)) %>%
  rbindlist(fill = TRUE)

stopifnot(rt_parsed %>%
            filter(Level == 'Bexar') %>%
            filter(is.na(Rt)) %>%
            nrow() < 10)

#   --------------------------------------------------------------------------------------------
rt_df_out = rt_parsed %>%
  filter(Date != max(Date)) %>%
  select(Level_Type, Level, Date, Rt, lower, upper) %>%
  arrange(Level_Type, Level, Date)


check_rt_combined_dupe = rt_df_out %>%
  group_by(Level_Type, Level, Date) %>%
  filter(n() > 1) %>%
  nrow() == 0

# diagnostics --------------------------------------------------------------------------------------------

rt_df_out %>%
  ggplot(aes(x = Date, y = Rt)) +
  geom_line() +
  geom_point() +
  facet_wrap(~Level, scales = 'free_y') +
  theme_bw()


# checks --------------------------------------------------------------------------------------------
arrow::write_parquet(rt_df_out, output_file)
print("Finished writing")