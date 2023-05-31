from dagster import asset
import pandas as pd
from src.wastewater.get_houston_plant_wastewater import (
    manage_houston_plant_wastewater
)

from src.wastewater.get_houston_zip_wastewater import (
    manage_houston_zip_wastewater
)


@asset(
    name="houston_plant",
    group_name="origin_wastewater",
    key_prefix=["origin", "wastewater"],
    metadata={
        "schema": "origin/wastewater",
        "table_name": "houston_plant",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_houston_wastewater_plant() -> pd.DataFrame:
    return manage_houston_plant_wastewater()


@asset(
    name="houston_zip",
    group_name="origin_wastewater",
    key_prefix=["origin", "wastewater"],
    metadata={
        "schema": "origin/wastewater",
        "table_name": "houston_zip",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_houston_wastewater_zip() -> pd.DataFrame:
    return manage_houston_zip_wastewater()
