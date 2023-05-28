from pathlib import Path

import pandas as pd
import yaml

from src.county_vitals.request_common import clean_request_data
from dagster import asset

from src.county_vitals.power_bi.get_power_bi import (
    parse_response,
    get_data,
)


def get_vitals(config) -> pd.DataFrame:
    print(f'Getting data for {config["county"]} County')
    response = get_data(config)
    result_df = parse_response(response, config)

    clean_df = clean_request_data(result_df, config)
    return clean_df


POWER_BI_CONFIG = yaml.safe_load(Path('src/county_vitals/power_bi/power_bi_config.yaml').read_text())


@asset(
    name="tarrant",
    group_name="vitals_power_bi",
    key_prefix=['vitals', 'power_bi'],
    metadata={
        "schema": "origin/vitals",
        "table_name": "tarrant_vitals",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_vitals_tarrant() -> pd.DataFrame:
    return get_vitals(POWER_BI_CONFIG['tarrant'])


@asset(
    name="el_paso",
    group_name="vitals_power_bi",
    key_prefix=['vitals', 'power_bi'],
    metadata={
        "schema": "origin/vitals",
        "table_name": "el_paso_vitals",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_vitals_el_paso() -> pd.DataFrame:
    return get_vitals(POWER_BI_CONFIG['el paso'])
