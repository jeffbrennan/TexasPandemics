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

select = dplyr::select
filter = dplyr::filter

# vaccinations --------------------------------------------------------------------------------------------
vaccination_file = fread('tableau/sandbox/county_daily_vaccine.csv')

Check_Vaccination_Counts = function(county_name) {
  # 2023-04-19 vaccine counts low boosted counts issue
  vaccination_count_check_prep = vaccination_file %>%
    filter(County == county_name) %>%
    group_by(Vaccination_Type) %>%
    arrange(desc(Date)) %>%
    select(Date, County, Vaccination_Type, At_Least_One_Dose, Fully_Vaccinated, Boosted, Population_5, Population_12, Population_16, Population_65)

  vaccination_count_check = vaccination_count_check_prep %>%
    select(Date, County, Vaccination_Type, At_Least_One_Dose, Fully_Vaccinated, Boosted) %>%
    pivot_longer(cols = c(At_Least_One_Dose, Fully_Vaccinated, Boosted), names_to = 'Vaccination_Group', values_to = 'Vaccination_Count')

  vaccination_check_plot = vaccination_count_check %>%
    ggplot(aes(x = Date, y = Vaccination_Count, color = Vaccination_Group)) +
    geom_line() +
    geom_point(size = 2) +
    facet_wrap(~Vaccination_Type,
               ncol = 1,
               scales = 'free') +
    ggpubr::theme_pubr()

  return(vaccination_check_plot)
}

Check_Vaccination_Counts('Bexar')
# county TPR --------------------------------------------------------------------------------------------
county_tpr = fread('tableau/county_TPR.csv')
glimpse(county_tpr)

county_tpr %>%
  filter(County == 'Harris') %>%
  mutate(Community_Level = factor(Community_Level, levels = c('', 'Low', 'Medium', 'High'))) %>%
  mutate(Community_Level_int = as.integer(Community_Level)) %>%
  ggplot(aes(x = Date, y = Community_Level_int, color = Community_Level)) +
  geom_line() +
  geom_point(size = 2) +
  ggpubr::theme_pubr()

county_tpr %>%
  filter(County == 'Harris') %>%
  filter(Date == max(Date)) %>%
  glimpse()


vaccination_file$Date %>% max()
county_tpr$Date %>% max()