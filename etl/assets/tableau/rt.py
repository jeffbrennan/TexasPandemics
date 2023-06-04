from dagster import asset, AssetIn
import pandas as pd
import pandera as pa
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.typing import Series, DataFrame
from src.utils import write_file


class RtFinal(pa.DataFrameModel):
    Level_Type: Series[pa.String] = pa.Field(description="Group descriptor - as of now just county")
    Level: Series[pa.String] = pa.Field(description="Actual level (ex Harris County)")
    Date: Series[pa.Date] = pa.Field(description="Date of observation", coerce=True)
    Rt: Series[pa.Float32] = pa.Field(description="Rt estimate")
    lower: Series[pa.Float32] = pa.Field(description="Lower bound of confidence interval")
    upper: Series[pa.Float32] = pa.Field(description="Upper bound of confidence interval")

    class Config:
        unique = ["Level_Type", "Level", "Date"]


@asset(
    name="rt",
    key_prefix=["tableau", "stats"],
    group_name="tableau",
    metadata={
        "schema": "tableau",
        "table_name": "rt",
        "add_archive": False
    },
    ins={
        'rt_raw':
            AssetIn(
                key=["intermediate", "rt", "rt_all"]
            )
    },
    dagster_type=pandera_schema_to_dagster_type(RtFinal),
    io_manager_key="pandas_io_manager"
)
def rt(rt_raw: pd.DataFrame) -> DataFrame[RtFinal]:
    rt_clean = (
        rt_raw.query("Level_Type != 'TMC'")
    )

    try:
        RtFinal.validate(rt_clean)
    except pa.errors.SchemaErrors as err:
        print(err.failure_cases)
        raise err

    write_file(rt_clean, "tableau/post_emergency/rt", add_date=False)
    return rt_clean
