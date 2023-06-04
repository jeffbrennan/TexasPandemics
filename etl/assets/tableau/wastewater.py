from dagster import asset, AssetIn
import pandas as pd
import pandera as pa
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.typing import Series, DataFrame
from src.utils import write_file


class WastewaterFinal(pa.DataFrameModel):
    County: Series[pa.String] = pa.Field(description="County")
    Date: Series[pa.Date] = pa.Field(description="Date of observation", coerce=True)
    viral_load: Series[pa.Float32] = pa.Field(description="Viral load estimate", nullable=True, coerce=True)
    source: Series[pa.String] = pa.Field(description="Source of data")

    class Config:
        unique = ["County", "Date"]


@asset(
    name="wastewater",
    key_prefix=["tableau", "wastewater"],
    group_name="tableau",
    metadata={
        "schema": "tableau",
        "table_name": "wastewater",
        "add_archive": False
    },
    ins={
        'combined_wastewater':
            AssetIn(
                key=["intermediate", "wastewater", "wastewater_combined"]
            )
    },
    dagster_type=pandera_schema_to_dagster_type(WastewaterFinal),
    io_manager_key="pandas_io_manager"
)
def wastewater(combined_wastewater: pd.DataFrame) -> DataFrame[WastewaterFinal]:
    try:
        WastewaterFinal.validate(combined_wastewater)
    except pa.errors.SchemaErrors as err:
        print(err.failure_cases)
        raise err

    write_file(combined_wastewater, "tableau/post_emergency/wastewater", add_date=False)
    return combined_wastewater
