# [UTHealth COVID-19 Dashboard](http://texaspandemic.org)

Scraping, statistics, and diagnostics of Texas COVID-19 data

2023-05-10 update: DSHS has ceased reporting county level updates on their original site. New state level data is available weekly here: https://www.dshs.texas.gov/covid-19-coronavirus-disease/texas-covid-19-surveillance

County level data is being individually scraped and aggregated in [tableau/vitals](tableau/vitals/)

---

## Core Data

| Level           |Frequency  |File                                                                                     | Description                                                             | Sources                                                                                                                      |
| :-------------  |:--------  |:-----                                                                                   |:-----                                                                   | :-----                                                                                                                       |
| County          | Daily     | [county.csv](tableau/county.csv)                    | Cases, Deaths, Testing, Mobility, Childcare facilities  | [DSHS](https://www.dshs.state.tx.us/coronavirus/additionaldata/) & [Google](https://www.google.com/covid19/mobility/)                                                                                  |
| County          | Weekly    | [county_TPR.csv](tableau/county_TPR.csv)                | Test Positivity Rates                            | [Centers for Medicare & Medicaid](https://data.cms.gov/stories/s/q5r5-gjyu)                                                                                                                            |
| TSA             | Daily     | [hospitalizations_tsa.csv](tableau/hospitalizations_tsa.csv)      | Hospitalizations, Bed Availability, Variants                     | [DSHS](https://www.dshs.state.tx.us/coronavirus/additionaldata/)                                                                                                                                       |
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

note: "stacked" indicates file includes statistics at County, TSA, PHR, Metro & State levels.

---

## Dashboard Element Sources

### **Vaccination**
![Vaccination plot #1](readme_images/vaccination_rate.png?raw=True)

- Fully Vaccinated: County level counts from [DSHS Vaccination Spreadsheet](https://dshs.texas.gov/immunize/covid19/COVID-19-Vaccine-Data-by-County.xls)
    - By County sheet
    - Column F: People Fully Vaccinated
    - Data is updated daily

![Vaccination plot #2](readme_images/vaccination_demo.png?raw=True)

- Population by Age: [Census County Population Characteristics (2019) CSV](https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/asrh/cc-est2019-agesex-48.csv)
    - < 12 Years: population Under 5 + 8/9ths of population aged 5-13
    - 12-15 years: 3/5ths of population aged 10-14 + 1/5th of population aged  15-19
    - Other age groups precalculated by the Census

- Population by Gender [Census County Population Characteristics (2019) CSV](https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/asrh/cc-est2019-agesex-48.csv)
    - Aggregated male and female counts for entire county population

- Population by Race:  [Census County Population Characteristics (2019) CSV](https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/asrh/cc-est2019-alldata-48.csv)

|Dashboard      |Census File  |
|:---           |:----        | 
|White          |NHWA (not hispanic white alone)     | 
|Asian          |NHAA (not hispanic asian alone)             |
|Black          |NHBA (not hispanic black or african american alone)            |
|Hispanic       |H  (Hispanic)           |
|Other          |TOM (two or more races) <br> NAC (Native Hawaiian and Other Pacific Islander alone or in combination) <br> IAC (American Indian and Alaska Native alone or in combination)            |

---

### **Hot Spots (TPR, Cases / 100k)**

![TPR plot](readme_images/hot_spot_tpr.png?raw=True)

- Test Positivity Rate: [Healthdata.gov Community Profile Report](https://beta.healthdata.gov/National/COVID-19-Community-Profile-Report/gqxm-d9w9)

    - Sheet: Counties | Column: AK (Viral lab test positivity rate - last 7 days) 

- Cases: [DSHS new confirmed cases spreadsheet](https://dshs.texas.gov/coronavirus/TexasCOVID-19NewCasesOverTimebyCounty.xlsx)

- County Population: [DSHS reported county population]('https://raw.githubusercontent.com/jeffbrennan/COVID-19/d03d476f7fb060dfd2e1a600a6a1e449df0ab8df/original-sources/DSHS_county_cases.csv')



---

### **Hot Spots (Case Ratio)**
![Case Ratio plot](readme_images/hot_spot_case_ratio.png?raw=True)

- Cases: [DSHS new confirmed cases spreadsheet](https://dshs.texas.gov/coronavirus/TexasCOVID-19NewCasesOverTimebyCounty.xlsx)

---

### **Cases & Fatalities**

![Cases Fatalities plot](readme_images/cases_fatalities.png?raw=True)


- Cases [DSHS new confirmed cases spreadsheet](https://dshs.texas.gov/coronavirus/TexasCOVID-19NewCasesOverTimebyCounty.xlsx)

- Fatalities: [DSHS reported fatalities spreadsheet](https://dshs.texas.gov/coronavirus/TexasCOVID19DailyCountyFatalityCountData.xlsx)

- Population: [DSHS reported county population]('https://raw.githubusercontent.com/jeffbrennan/COVID-19/d03d476f7fb060dfd2e1a600a6a1e449df0ab8df/original-sources/DSHS_county_cases.csv')



---

### **Critical Trends**

![Case plot](readme_images/critical_cases.png?raw=True)

- Cases: [DSHS new confirmed cases spreadsheet](https://dshs.texas.gov/coronavirus/TexasCOVID-19NewCasesOverTimebyCounty.xlsx)

![Pct plot](readme_images/critical_pct.png?raw=True)

- Cases % diff: [DSHS new confirmed cases spreadsheet](https://dshs.texas.gov/coronavirus/TexasCOVID-19NewCasesOverTimebyCounty.xlsx)
- Tests % diff: [CMS Testing Archive](https://data.cms.gov/stories/s/q5r5-gjyu)
    - Column G: Testing in prior 14 days

![Prediction plot](readme_images/critical_prediction.png?raw=True)

- Cases: [DSHS new confirmed cases spreadsheet](https://dshs.texas.gov/coronavirus/TexasCOVID-19NewCasesOverTimebyCounty.xlsx)

- ARIMA Model
    - forecast::auto.arima() on 7 day moving average of new cases

![Rt plot](readme_images/critical_rt.png?raw=True)

- Cases: [DSHS new confirmed cases spreadsheet](https://dshs.texas.gov/coronavirus/TexasCOVID-19NewCasesOverTimebyCounty.xlsx)

- RT Estimate
    - R0::estimate.R()
        - nsim=1000
        - methods="TD"
        - GT = R0::generation.time("gamma", c(3.96, 4.75))

---

### **Hospitalization**

![Hospitalizations plot](readme_images/hosp_daily.png?raw=True)

- Hospitalizations: [DSHS TSA Level spreadsheet](https://dshs.texas.gov/coronavirus/CombinedHospitalDataoverTimebyTSA.xlsx)
    - Sheet: COVID-19 Hospitalizations 

![Ventilator plot](readme_images/hosp_vent.png?raw=True)

- Ventilators: [DSHS COVID Hospitalization dashboard](https://txdshs.maps.arcgis.com/apps/dashboards/0d8bdf9be927459d9cb11b9eaef6101f)



![Bed plot](readme_images/hosp_beds.png?raw=True)

- ICU Hospitalizations
    - Available: Sheet #5
    - COVID: Sheet #9
    - Occupied: Sheet #11
    - Occupied - other: Occupied - COVID

- General
    - Available: Sheet #4
    - COVID: Sheet #8
    - Occupied: Sheet #10
    - Occupied - Other: Occupied - COVID

---

### **Mobility**

![Mobility plot](readme_images/mobility.png?raw=True)

- [Google mobility CSV (~ 500 mb file)](https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv)
    - Filtered sub_region_1 for Texas
    - Data columns: J:O (retail & recreation: residential)
---

### **Demographics**

![Demo plot](readme_images/demo.png?raw=True)

- Case & Fatality demographics: [DSHS demographics spreadsheet](https://dshs.texas.gov/coronavirus/TexasCOVID19Demographics.xlsx)
    - Combined data from all sheets
    - Snapshot downloaded after weekly update and aggregated together
    - [Snapshot archive location](https://github.com/jeffbrennan/COVID-19/tree/master/original-sources/historical/demo-archive)

![Demo state plot](readme_images/demo_state.png?raw=True)

- State level demographics: [Census quickfacts](https://www.census.gov/quickfacts/TX)
- County level population estimates: [Census - County Population Totals](https://www.census.gov/data/tables/time-series/demo/popest/2020s-counties-total.html)