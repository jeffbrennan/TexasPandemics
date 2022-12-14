library(rmarkdown)

base_wd = "C:/Users/jeffb/Desktop/Life/personal-projects/COVID"
setwd(base_wd)

Sys.setenv(RSTUDIO_PANDOC = 'C:/Program Files/RStudio/bin/quarto/bin/tools')
render('src/covid-scraping.rmd', output_dir = 'diagnostics/', knit_root_dir = base_wd)
render('src/statistical-analysis.rmd', output_dir = 'diagnostics/', knit_root_dir = base_wd)
render('src/diagnostics.rmd', output_dir = 'diagnostics/', knit_root_dir = base_wd)
