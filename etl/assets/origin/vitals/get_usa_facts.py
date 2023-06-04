import pandas as pd
from src.county_vitals.usa_facts.get_usa_fact_vitals import (
    get_vitals, combine_vitals
)
from pathlib import Path
import yaml
from dagster import asset

USA_FACTS_CONFIG = yaml.safe_load(Path('src/county_vitals/usa_facts/usa_facts_config.yaml').read_text())


@asset(
    name="usa_facts",
    group_name="origin_vitals",
    key_prefix=['origin', 'vitals'],
    metadata={
        "schema": "origin/vitals/other_sources",
        "table_name": "usa_facts_vitals",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_usa_facts_vitals() -> pd.DataFrame:
    results = [get_vitals(USA_FACTS_CONFIG[x]) for x in ['cases', 'deaths']]
    final_df = combine_vitals(results)

    # TODO: add newness check, only write if new

    return final_df

