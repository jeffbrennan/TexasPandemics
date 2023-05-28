import pandas as pd
from tableauscraper import TableauScraper as TS
from tableauscraper import TableauWorkbook
from src.utils import write_file
import yaml
from pathlib import Path
from src.county_vitals.request_common import clean_request_data
from dagster import asset
from src.county_vitals.tableau.get_tableau_data import (
    get_data,
    create_workbook,
)


def get_vitals(config: dict) -> pd.DataFrame:
    workbook = create_workbook(config['url'])
    raw_data = get_data(workbook, config)

    clean_df = clean_request_data(raw_data, config)
    return clean_df


TABLEAU_CONFIG = yaml.safe_load(Path('src/county_vitals/tableau/tableau_config.yaml').read_text())


@asset(
    name="galveston",
    group_name="vitals_tableau",
    key_prefix=['vitals', 'tableau'],
    metadata={
        "schema": "origin/vitals",
        "table_name": "galveston_vitals",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_vitals_galveston() -> pd.DataFrame:
    return get_vitals(TABLEAU_CONFIG['galveston'])
