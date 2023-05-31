import pandas as pd
from dagster import asset

from src.wastewater.get_cdc_wastewater import (
    create_client,
    create_offsets,
    get_data,
    clean_data
)

from src.utils import load_csv


@asset(
    name="cdc_wastewater",
    group_name="origin_wastewater",
    key_prefix=['origin', 'wastewater'],
metadata={
        "schema": "origin/wastewater",
        "table_name": "cdc_wastewater",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_cdc_wastewater() -> pd.DataFrame:
    DATASET_ID = "2ew6-ywp6"
    client = create_client()

    # TODO: update path
    current_df = load_csv('tableau/wastewater/cdc_wastewater.csv')
    current_max_date = current_df['Date'].max()

    # region pull data --------------------------------------------------------------------------------
    offsets = create_offsets(client, DATASET_ID, current_max_date)
    results = [get_data(client, offset, DATASET_ID, current_max_date) for offset in offsets]
    results_df = pd.concat([pd.DataFrame.from_records(i) for i in results])
    # endregion

    # region clean_data --------------------------------------------------------------------------------
    clean_df = clean_data(results_df)
    return clean_df
