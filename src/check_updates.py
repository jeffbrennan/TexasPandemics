import polars as pl
wastewater = pl.read_csv('tableau/post_emergency/wastewater.csv')

# get max date per county
(
    wastewater
    .groupby(['County'])
    .agg(pl.max('Date'))
)

(
    wastewater
    .groupby('source')
    .agg(pl.max('Date'))
)