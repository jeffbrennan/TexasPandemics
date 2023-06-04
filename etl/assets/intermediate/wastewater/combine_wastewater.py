from dagster import asset, AssetIn
import pandas as pd
from src.utils import union_df_list


@asset(
    name="wastewater_combined",
    key_prefix=["intermediate", "wastewater"],
    group_name="intermediate_wastewater",
    metadata={
        "schema": "intermediate/wastewater",
        "table_name": "wastewater_combined",
        "add_archive": False
    },
    ins={
        'biobot':
            AssetIn(
                key=["origin", "wastewater", "biobot_wastewater"]
            ),
        'houston_plant':
            AssetIn(
                key=["origin", "wastewater", "houston_plant"]
            ),
        'houston_zip':
            AssetIn(
                key=["origin", "wastewater", "houston_zip"]
            )
    },

    io_manager_key="pandas_io_manager"
)
def combine_wastewater(biobot, houston_plant, houston_zip) -> pd.DataFrame:
    houston_wastewater = (
        pd.concat(
            [
                houston_plant[['Date', 'viral_load_log10']],
                houston_zip[['Date', 'viral_load_log10']]
            ]
        )
        .groupby('Date')
        .mean()
        # approximate biobot measurement
        .assign(viral_load_log10=lambda x: x['viral_load_log10'] * 10)
        .assign(County="Harris")
        .assign(source='houston_wastewater_dashboard')
        .rename(columns={"viral_load_log10": "viral_load"})
        .reset_index()
    )

    biobot_clean = (
        biobot
        [['County', 'Date', 'viral_copies_per_ml']]
        .rename(columns={"viral_copies_per_ml": "viral_load"})
        .assign(source='biobot')
    )

    # favor biobot over cdc
    combined_df = (
        union_df_list([biobot_clean, houston_wastewater])
        [['County', 'Date', 'viral_load', 'source']]
    )

    return combined_df
