import pandas as pd
from dagster import asset

from src.county_vitals.combine_vitals import (
    list_files,
    load_files,
    combine_vitals,
    run_diagnostics,
    clean_vitals
)


@asset(
    name="vitals_combined",
    key_prefix=["intermediate"],
    group_name="intermediate",
    metadata={
        "schema": "intermediate",
        "table_name": "vitals_combined",
        "add_archive": True
    },

    non_argument_deps={
        # arcgis
        'vitals/arcgis/bexar',
        'vitals/arcgis/harris',
        'vitals/arcgis/travis',
        'vitals/arcgis/randall',
        'vitals/arcgis/potter',
        'vitals/arcgis/denton',
        'vitals/arcgis/nueces',

        # power bi
        'vitals/power_bi/tarrant',
        'vitals/power_bi/el_paso',

        # tableau
        'vitals/tableau/galveston',
    },
    io_manager_key="pandas_io_manager"
)
def get_vitals_combined() -> pd.DataFrame:
    county_files = list_files()
    county_files_combined_raw = load_files(county_files)
    county_files_combined = combine_vitals(county_files_combined_raw)
    county_files_clean = clean_vitals(county_files_combined)
    return county_files_clean
