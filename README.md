# [UTHealth COVID-19 Dashboard](http://texaspandemic.org)

Scraping, statistics, and diagnostics of Texas COVID-19 data

---

## Core Data

| Level           |Frequency  |File                                                                                     | Description                                                             | Sources                                                                                                                      |
| :-------------  |:--------  |:-----                                                                                   |:-----                                                                   | :-----                                                                                                                       |
| School District | Weekly    | [district_school_reopening.csv](tableau/district_school_reopening.csv)                  | ISD Cases & Infections                                            | [DSHS](https://www.dshs.state.tx.us/coronavirus/additionaldata/) & [TEA](https://schoolsdata2-tea-texas.opendata.arcgis.com/)|
| County          | Daily     | [county.csv](tableau/county.csv)                    | Cases, Deaths, Testing, Mobility, Childcare facilities  | [DSHS](https://www.dshs.state.tx.us/coronavirus/additionaldata/) & [Google](https://www.google.com/covid19/mobility/)                                                                                  |
| County          | Weekly    | [county_TPR.csv](tableau/county_TPR.csv)                | Test Positivity Rates                            | [Centers for Medicare & Medicaid](https://data.cms.gov/stories/s/q5r5-gjyu)                                                                                                                            |
| TSA             | Daily     | [hospitalizations_tsa.csv](tableau/hospitalizations_tsa.csv)      | Hospitalizations & Bed Availability                     | [DSHS](https://www.dshs.state.tx.us/coronavirus/additionaldata/)                                                                                                                                       |
| State           | Weekly    | [stacked_demographics.csv](tableau/stacked_demographics.csv)      | Demographics from Case & Fatality Investigations        | [DSHS](https://dshs.texas.gov/coronavirus/additionaldata/)

---

## Statistics

| File                                                                | Description
| :---------                                                          | :-----------------
| [stacked_case_ratio.csv](tableau/stacked_case_ratio.csv)            | 7 day average in past week vs 2 weeks ago
| [stacked_case_timeseries.csv](tableau/stacked_case_timeseries.csv)  | ARIMA forecast model on new daily cases
| [stacked_hosp_timeseries.csv](tableau/stacked_hosp_timeseries.csv)  | ARIMA forecast model on new hospitalizations
| [stacked_pct_change.csv](tableau/stacked_pct_change.csv)            | Spline case and test % change
| [stacked_rt.csv](tableau/stacked_rt.csv)                            | Rt estimates
| [stacked_critical_trends.csv](tableau/stacked_critical_trends.csv)  | All statistics stacked together

note: "stacked" indicates file includes statistics at County, TSA, PHR, Metro & State levels. Rt also includes school district estimates.

---