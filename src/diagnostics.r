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
glimpse(vaccination_file)

# 2023-04-19 vaccine counts low boosted counts issue
vaccination_file %>%
  filter(County == 'Harris') %>%
  group_by(Vaccination_Type, Date) %>%
  summarize(across(c(At_Least_One_Dose, Fully_Vaccinated, Boosted), ~sum(., na.rm = TRUE))) %>%
  arrange(desc(Date)) %>%
  slice(1:10)

# max date 2/22/2023?
vaccination_file$Date %>% max()


# county TPR --------------------------------------------------------------------------------------------


county_tpr = fread('tableau/county_TPR.csv')