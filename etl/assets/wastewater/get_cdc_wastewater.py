import pandas as pd
from dagster import asset

from src.wastewater.get_cdc_wastewater import (
    create_client,
    create_offsets,
    get_data,
    clean_data
)


@asset(
    name="cdc_wastewater",
    group_name="wastewater",
    key_prefix=['vitals', 'tableau'],
    metadata={
        "schema": "vitals",
        "table_name": "galveston_vitals",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_cdc_wastewater() -> pd.DataFrame:
    DATASET_ID = "2ew6-ywp6"
    client = create_client()
    # region pull data --------------------------------------------------------------------------------
    offsets = create_offsets(client, DATASET_ID)
    results = [get_data(offset, DATASET_ID) for offset in offsets]
    results_df = pd.concat([pd.DataFrame.from_records(i) for i in results])
    # endregion

    # region clean_data --------------------------------------------------------------------------------
    clean_df = clean_data(results_df)
    return clean_df
