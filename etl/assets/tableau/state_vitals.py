from dagster import asset, AssetIn
import pandas as pd
import pandera as pa
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.typing import Series, DataFrame
from src.utils import write_file


class StateVitals(pa.DataFrameModel):
    Date: Series[pa.Date] = pa.Field(description="Date of observation", coerce=True)
    Level_Type: Series[pa.String] = pa.Field(description="Level of observation (ex State)")
    Level: Series[pa.String] = pa.Field(description="Level of observation (ex Texas)")
    new_cases_probable_plus_confirmed: Series[pa.Int32] = pa.Field(
        description="New cases probable plus confirmed",
        ge=0
    )
    new_cases_confirmed: Series[pa.Int32] = pa.Field(description="New cases confirmed", ge=0)
    new_cases_probable: Series[pa.Int32] = pa.Field(description="New cases probable", ge=0)
    cumulative_cases_probable_plus_confirmed: Series[pa.Int32] = pa.Field(
        description="Cumulative cases probable+confirmed", ge=0
    )
    cumulative_cases_confirmed: Series[pa.Int32] = pa.Field(description="Cumulative cases confirmed", ge=0)
    cumulative_cases_probable: Series[pa.Int32] = pa.Field(description="Cumulative cases probable", ge=0)
    new_deaths: Series[pa.Int32] = pa.Field(description="New deaths", ge=0, nullable=True)
    new_hospitalizations: Series[pa.Int32] = pa.Field(description="New hospitalizations", ge=0, nullable=True)
    hospitalizations_7_day: Series[pa.Float32] = pa.Field(description="Hospitalizations 7 day", ge=0, nullable=True)


    class Config:
        unique = ["Level", "Date"]


@asset(
    name="state_vitals",
    key_prefix=['tableau', 'vitals'],
    group_name="tableau",
    ins={
        "state_vitals_combined": AssetIn(
            key=["intermediate", "vitals", "state_vitals_combined"],
        )
    },
    metadata={
        "schema": "tableau",
        "table_name": "state_vitals",
        "add_archive": False
    },
    dagster_type=pandera_schema_to_dagster_type(StateVitals),
    io_manager_key='pandas_io_manager'
)
def state_vitals(state_vitals_combined: pd.DataFrame) -> DataFrame[StateVitals]:
    state_vitals = state_vitals_combined
    state_vitals = (
        state_vitals
        .astype(
            {
                'new_cases_probable_plus_confirmed': 'Int32',
                'new_cases_confirmed': 'Int32',
                'new_cases_probable': 'Int32',
                'cumulative_cases_probable_plus_confirmed': 'Int32',
                'cumulative_cases_confirmed': 'Int32',
                'cumulative_cases_probable': 'Int32',
                'new_deaths': 'Int32',
                'new_hospitalizations': 'Int32',
                'hospitalizations_7_day': 'float32'
            }
        )
    )
    try:
        StateVitals.validate(state_vitals)
    except pa.errors.SchemaErrors as err:
        print(err.failure_cases)
        raise err

    write_file(state_vitals, "tableau/post_emergency/state_vitals", add_date=False)
    return state_vitals
