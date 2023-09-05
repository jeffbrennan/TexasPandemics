from dagster import asset, AssetIn
import pandas as pd
import pandera as pa
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.typing import Series, DataFrame
from src.utils import write_file


class WastewaterPlantFinal(pa.DataFrameModel):
    County: Series[pa.String] = pa.Field(description="County")
    Plant_Name: Series[pa.String] = pa.Field(description="Plant Name")
    Date: Series[pa.Date] = pa.Field(description="Date of observation", coerce=True)
    viral_load: Series[pa.Float32] = pa.Field(description="Viral load estimate", nullable=True, coerce=True)
    viral_load_log10: Series[pa.Float32] = pa.Field(description="Viral load estimate (log10)", nullable=True, coerce=True)
    source: Series[pa.String] = pa.Field(description="Source of data")

    class Config:
        unique = ["County", "Plant_Name", "Date"]


@asset(
    name="wastewater_plant",
    key_prefix=["tableau", "wastewater"],
    group_name="tableau",
    metadata={
        "schema": "tableau",
        "table_name": "wastewater_plant",
        "add_archive": False
    },
    ins={
        'wastewater_plant_combined':
            AssetIn(
                key=["intermediate", "wastewater", "wastewater_plant_combined"]
            )
    },
    dagster_type=pandera_schema_to_dagster_type(WastewaterPlantFinal),
    io_manager_key="pandas_io_manager"
)
def wastewater_plant(wastewater_plant_combined: pd.DataFrame) -> DataFrame[WastewaterPlantFinal]:
    try:
        WastewaterPlantFinal.validate(wastewater_plant_combined)
    except pa.errors.SchemaErrors as err:
        print(err.failure_cases)
        raise err

    write_file(wastewater_plant_combined, "tableau/post_emergency/wastewater_plant", add_date=False)
    return wastewater_plant_combined
