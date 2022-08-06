# setup ---------------------------------------------------------------------------------------
suppressMessages(suppressWarnings(source("C:/Users/jeffb/Desktop/Life/personal-projects/functions/util.R")))
date_out = ifelse((Sys.time() < as.POSIXct(paste0(Sys.Date(), '16:00'), tz = 'America/Chicago')),
                  Sys.Date() - 1,
                  Sys.Date()) |> 
  as.Date(origin = '1970-01-01')

setwd('C:/Users/jeffb/Desktop/Life/personal-projects/COVID/')

# dshs ----------------------------------------------------------------------------------------

## functions -----------------------------------------------------------------------------------
Run_Diagnostics = function(df) {
  na_check = nrow(df) == df |> filter(!is.na(Cases)) |> nrow()
  checks = all(c(na_check))
  
  return(checks)
}

Get_Demographics = function() { 
  dshs_demographics = age_table |> 
    rename(Demo_Group = `Age Category`,
           Cases = `Number of Cases`)  |>
    mutate(Demo = 'Age') |> 
    plyr::rbind.fill(
      gender_table |>
        rename(Demo_Group = Sex,
               Cases = `Number of Cases`) |>
        mutate(Demo = 'Gender')
    ) |> 
    filter(Demo_Group != 'Total') |> 
    relocate(Demo, .before = Demo_Group) |> 
    mutate(Date = dshs_page[['update']]) |> 
    relocate(Date, .before = Demo) |> 
    mutate(Demo_Group = as.character(Demo_Group))
  
  checks = Run_Diagnostics(dshs_demographics)
  
  combined_demo = fread('monkeypox/stacked_state_demographics.csv') |> 
    plyr::rbind.fill(dshs_demographics) |> 
    distinct() |> 
    mutate(Date = as_date(Date, format = '%Y-%m-%d')) |> 
    arrange(Demo, Demo_Group, Date)
  
  if(checks) { 
    message('Writing demographics update')
    fwrite(combined_demo, glue('monkeypox/stacked_state_demographics.csv'))
    }
  return(dshs_demographics)
}

Get_Cases = function() {
  phr_table_clean = phr_table |> 
    mutate(Date = dshs_page[['update']]) |> 
    rename(PHR = `Public Health Region`,
           Cases = `Number of Cases`)
  
  dshs_cases = phr_table_clean |>
    rename(Level = PHR) |> 
    filter(Level != 'Total') |>
    mutate(Level_Type = 'PHR') |> 
    plyr::rbind.fill(
      phr_table_clean |>
        rename(Level = PHR) |> 
        filter(Level == 'Total') |>
        mutate(Level_Type = 'State') |> 
        mutate(Level = 'Texas')
    ) |> 
    relocate(Level_Type, .before = Level) |> 
    relocate(Date, .before = Level_Type)
  
  checks = Run_Diagnostics(dshs_cases)
  
  cases_combined = fread('monkeypox/stacked_cases.csv') |> 
    plyr::rbind.fill(dshs_cases) |> 
    mutate(Date = as_date(Date, format = '%Y-%m-%d')) |>
    arrange(Level, Level_Type, Date) |> 
    distinct()
  
  
  if(checks) { 
    message('Writing case update')
    fwrite(cases_combined, glue('monkeypox/stacked_cases.csv'))
  }
  return(dshs_cases)
}

Check_DSHS = function() { 
  
  dshs_cases_html = read_html('https://www.dshs.texas.gov/news/updates.shtm#monkeypox')
  dshs_case_update = dshs_cases_html |> 
    html_nodes(xpath = '//*[@id="ctl00_ContentPlaceHolder1_ContentPageColumnCenter"]/span[2]') |> 
    html_text() |> 
    str_squish() %>% 
    as_date(., format = '%B %d, %Y')
  
  
  return(list('html' = dshs_cases_html,
              'update' = dshs_case_update)
         )
}



## setup ---------------------------------------------------------------------------------------
CURRENT_MAX_DATE = fread('monkeypox/stacked_state_demographics.csv') |> 
  pull(Date) %>% 
  as_date(., format = '%m/%d/%Y') |> 
  max()

MAX_ATTEMPTS = 50
SLEEP_TIME = 60 * 10

dshs_page = Check_DSHS()

message('Checking https://www.dshs.texas.gov/news/updates.shtm#monkeypox for updates... ')


## check ---------------------------------------------------------------------------------------

counter = 1
while (CURRENT_MAX_DATE >= dshs_page[['update']] & counter < MAX_ATTEMPTS) {
  message(glue('[ATTEMPT #{str_pad(counter, width = 2, pad = "0")}] Retrying in {SLEEP_TIME / 60} minutes...'))
  Sys.sleep(SLEEP_TIME)
  dshs_page = Check_DSHS()
  counter = counter + 1 
}


## write ---------------------------------------------------------------------------------------
if (CURRENT_MAX_DATE < dshs_page[['update']]) {
  all_tables = dshs_page[['html']] %>% html_table()
  age_table = all_tables[map(all_tables, ~any(str_detect(names(.), 'Age'))) %>% unlist()][[1]]
  gender_table = all_tables[map(all_tables, ~any(str_detect(names(.), 'Sex'))) %>% unlist()][[1]]
  phr_table = all_tables[map(all_tables, ~any(str_detect(names(.), 'Public Health Region'))) %>% unlist()][[1]]
  
  dshs_demographics = Get_Demographics()
  dshs_cases = Get_Cases()
}
