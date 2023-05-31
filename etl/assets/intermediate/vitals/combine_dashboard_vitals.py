import pandas as pd
from dagster import asset

from src.county_vitals.combine_vitals import (
    list_files,
    load_files,
    combine_vitals,
    clean_vitals
)


@asset(
    name="dashboard_vitals_combined",
    key_prefix=["vitals", "intermediate"],
    group_name="intermediate_vitals",
    metadata={
        "schema": "intermediate/vitals",
        "table_name": "dashboard_vitals_combined",
        "add_archive": True
    },

    non_argument_deps={
        # arcgis
        'origin/vitals/bexar',
        'origin/vitals/harris',
        'origin/vitals/travis',
        'origin/vitals/randall',
        'origin/vitals/potter',
        'origin/vitals/denton',
        'origin/vitals/nueces',

        # power bi
        'origin/vitals/tarrant',
        'origin/vitals/el_paso',

        # tableau
        'origin/vitals/galveston',
    },
    io_manager_key="pandas_io_manager"
)
def dashboard_vitals_combined() -> pd.DataFrame:
    county_files = list_files()
    county_files_combined_raw = load_files(county_files)
    county_files_combined = combine_vitals(county_files_combined_raw)
    county_files_clean = clean_vitals(county_files_combined)
    return county_files_clean
