from dagster import asset, AssetIn
import pandas as pd
import pandera as pa
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.typing import Series, DataFrame

dashboard_vitals = pd.read_parquet('data/intermediate/vitals/dashboard_vitals_combined.parquet')
usa_vitals = pd.read_parquet('data/origin/vitals/usa_facts_vitals.parquet')


def combine_wastewater(df_list: list) -> pd.DataFrame:
    combined_df = (
        pd.concat(df_list)
        .reset_index(drop=True)
        .assign(Date=lambda df: pd.to_datetime(df.Date))
    )
    return combined_df


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
        'cdc_wastewater':
            AssetIn(
                key=["origin", "wastewater", "cdc_wastewater"]
            ),
        'houston_wastewater_plant':
            AssetIn(
                key=["origin", "wastewater", "houston_plant"]
            ),
        'houston_wastewater_zip':
            AssetIn(
                key=["origin", "wastewater", "houston_zip"]
            )
    },
    io_manager_key="pandas_io_manager"
)
def combine_wastewater(
        cdc_wastewater: pd.DataFrame,
        houston_wastewater_plant: pd.DataFrame,
        houston_wastewater_zip: pd.DataFrame
) -> None:
    # TODO: implement combination
    combined_df = combine_wastewater([cdc_wastewater, houston_wastewater_plant, houston_wastewater_zip])
    return combined_df
