from dagster import asset, AssetIn
import pandas as pd

@asset(
    name="wastewater_plant_combined",
    key_prefix=["intermediate", "wastewater"],
    group_name="intermediate_wastewater",
    metadata={
        "schema": "intermediate/wastewater",
        "table_name": "wastewater_plant_combined",
        "add_archive": False
    },
    ins={
        'houston_plant':
            AssetIn(
                key=["origin", "wastewater", "houston_plant"]
            ),
    },

    io_manager_key="pandas_io_manager"
)
def wastewater_plant_combined(houston_plant) -> pd.DataFrame:
    houston_plant = pd.read_parquet('data/origin/wastewater/houston_plant.parquet')

    cleaned_df = (
        houston_plant
        .assign(source='houston_wastewater_dashboard')
        .drop_duplicates()
        [['County', 'Plant_Name', 'Date', 'viral_load', 'viral_load_log10', 'source']]
    )
    return cleaned_df
