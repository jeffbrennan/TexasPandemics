from dagster import asset, AssetIn
import pandas as pd
import pandera as pa
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.typing import Series, DataFrame


class VitalsCombined(pa.DataFrameModel):
    County: Series[pa.String] = pa.Field(description="Texas county name")
    Date: Series[pa.Date] = pa.Field(description="Date of observation", coerce=True)
    cases_cumulative: Series[pa.Int32] = pa.Field(description="Cumulative number of cases")
    deaths_cumulative: Series[pa.Int32] = pa.Field(description="Cumulative number of deaths")
    cases_daily: Series[pa.Int32] = pa.Field(description="Daily number of cases")
    deaths_daily: Series[pa.Int32] = pa.Field(description="Daily number of deaths")


def combine_vitals(vital_list: list) -> pd.DataFrame:
    combined_df = (
        pd.concat(vital_list)
        .reset_index(drop=True)
        .assign(Date=lambda df: pd.to_datetime(df.Date))
        .sort_values(by=['County', 'Date'])
    )
    return combined_df


@asset(
    name="all_county_vitals_combined",
    key_prefix=["vitals", "intermediate"],
    group_name="intermediate_vitals",
    metadata={
        "schema": "intermediate/vitals",
        "table_name": "all_county_vitals_combined",
        "add_archive": False
    },
    ins={
        'dashboard_combined_vitals':
            AssetIn(
                key=["vitals", "intermediate", "dashboard_vitals_combined"]
            ),
        # 'usa_facts_vitals':
            # AssetIn(
                # key=["vitals", "intermediate", "usa_facts"]
            # )
    },
    dagster_type=pandera_schema_to_dagster_type(VitalsCombined),
    io_manager_key="pandas_io_manager"
)
def all_county_vitals_combined(
        dashboard_combined_vitals: pd.DataFrame
        # usa_facts_vitals: pd.DataFrame
) -> DataFrame[VitalsCombined]:
    # usa_facts not consistent with counts, also doesn't seem to be updated
    combined_df = dashboard_combined_vitals
    return combined_df
