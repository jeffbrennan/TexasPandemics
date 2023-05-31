from dagster import asset, AssetIn
from src.county_vitals.usa_facts.get_usa_fact_vitals import (
    clean_vitals
)
import pandas as pd


@asset(
    name="usa_facts",
    group_name="intermediate_vitals",
    key_prefix=['vitals', 'intermediate'],
    metadata={
        "schema": "intermediate/vitals",
        "table_name": "usa_facts_vitals_clean",
        "add_archive": True
    },
    ins={
        'usa_fact_vitals':
            AssetIn(
                key=["origin", "vitals", "usa_facts"]
            )
    },

    io_manager_key='pandas_io_manager'
)
def clean_usa_fact_vitals(usa_fact_vitals) -> pd.DataFrame:
    clean_df = clean_vitals(usa_fact_vitals)
    return clean_df