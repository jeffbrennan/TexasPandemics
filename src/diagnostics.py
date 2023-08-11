# wastewater file stats
import pandas as pd
import os
wastewater_df = pd.read_parquet('data/tableau/wastewater.parquet')

# houston wastewater lower than expectd
(
    wastewater_df
    .groupby('source')
    .agg({'Date': ['min', 'max']})
)



# travis maxdate 7/12?
(
    wastewater_df
    .groupby('County')
    .agg({'Date': ['min', 'max']})
)



# rt

rt = pd.read_parquet('data/tableau/rt.parquet')
rt.Date.max()

# vitals
vitals = pd.read_parquet('data/tableau/county_vitals.parquet')
vitals.head()


(
    vitals
    .groupby('County')
    .agg({'Date': ['min', 'max']})
)

(
    vitals
    .query('County == "Harris"')
    [['Date', 'cases_daily']]
)