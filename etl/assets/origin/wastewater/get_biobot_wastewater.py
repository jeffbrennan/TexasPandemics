import pandas as pd
from dagster import asset

from src.wastewater.get_biobot_wastewater import (
    obtain_urls,
    clean_data

)

@asset(
    name="biobot_wastewater",
    group_name="origin_wastewater",
    key_prefix=['origin', 'wastewater'],
    metadata={
        "schema": "origin/wastewater",
        "table_name": "biobot_wastewater",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_biobot_wastewater() -> pd.DataFrame:
    wastewater_run_data = obtain_urls(1)
    raw_data = [pd.read_csv(i) for i in wastewater_run_data['urls']]
    cleaned_biobot_data = pd.concat([clean_data(i) for i in raw_data]).drop_duplicates()
    return cleaned_biobot_data
