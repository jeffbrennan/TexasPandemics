from dagster import asset, AssetIn
import pandas as pd
import pandera as pa
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.typing import Series, DataFrame
from src.utils import write_file


class CountyVitals(pa.DataFrameModel):
    County: Series[pa.String] = pa.Field(description="Texas county name")
    Date: Series[pa.Date] = pa.Field(description="Date of observation", coerce=True)
    cases_cumulative: Series[pa.Int32] = pa.Field(description="Cumulative number of cases", ge=0)
    deaths_cumulative: Series[pa.Int32] = pa.Field(description="Cumulative number of deaths", ge=0)
    cases_daily: Series[pa.Int32] = pa.Field(description="Daily number of cases", ge=0)
    deaths_daily: Series[pa.Int32] = pa.Field(description="Daily number of deaths", ge=0)

    @pa.check("cases_cumulative", groupby="County", name="cases_cumulative_increasing")
    def validate_increasing_cases(cls, x: dict[str, Series[pa.Int32]]) -> bool:
        results = [cases.is_monotonic_increasing for county, cases in x.items()]
        return all(results)

    @pa.check("deaths_cumulative", groupby="County", name="deaths_cumulative_increasing")
    def validate_increasing_deaths(cls, x: dict[str, Series[pa.Int32]]) -> bool:
        results = [deaths.is_monotonic_increasing for county, deaths in x.items()]
        return all(results)

    class Config:
        unique = ["County", "Date"]


@asset(
    name="county_vitals",
    key_prefix=["vitals"],
    group_name="tableau",
    metadata={
        "schema": "tableau",
        "table_name": "county_vitals",
        "add_archive": False
    },
    ins={
        'combined_vitals':
            AssetIn(
                key=["vitals", "intermediate", "vitals_combined"]
            )
    },
    dagster_type=pandera_schema_to_dagster_type(CountyVitals),
    io_manager_key="pandas_io_manager"
)
def county_vitals(combined_vitals: pd.DataFrame) -> DataFrame[CountyVitals]:
    county_vitals_df = combined_vitals

    try:
        CountyVitals.validate(county_vitals_df)
    except pa.errors.SchemaErrors as err:
        print(err.failure_cases)
        raise err

    write_file(county_vitals_df, "tableau/county_vitals", add_date=False)
    return county_vitals_df
