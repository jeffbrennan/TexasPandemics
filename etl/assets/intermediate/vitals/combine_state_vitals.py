import pandas as pd
from dagster import asset, AssetIn

from src.get_covid_dshs import (
    combine_with_existing
)


@asset(
    name="texas_vitals",
    group_name="intermediate_vitals",
    key_prefix=['intermediate'],
    ins={
        "new_texas_vitals": AssetIn(
            key=["origin", "vitals", "new_texas_vitals"],
        )
    },
    metadata={
        "schema": "intermediate",
        "table_name": "state_vitals_combined",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
# TODO: update existing path to reference self
def combine_state_vitals(new_texas_vitals) -> pd.DataFrame:
    existing_vitals = pd.read_csv('tableau/state_vitals.csv')
    combined_df = combine_with_existing(new_texas_vitals, existing_vitals)
    return combined_df
