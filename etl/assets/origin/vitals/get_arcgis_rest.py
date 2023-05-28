from functools import reduce
from pathlib import Path

import pandas as pd
import yaml
from dagster import asset

from src.county_vitals.arcgis_rest.get_arcgis_rest import (
    parse_data_manager,
    create_config_list,

)

def get_vitals_manager(config: dict) -> pd.DataFrame:
    print(f'================Obtaining {config["data_type"]} - {config["county"]}==============================')
    config_list = create_config_list(config)

    all_clean_data = []
    for config in config_list:
        clean_data_raw = parse_data_manager(config)
        all_clean_data.append(clean_data_raw)

    if len(all_clean_data) > 1:
        clean_df = reduce(
            lambda x, y: pd.merge(x, y, on=['County', 'Date'], how='outer'),
            all_clean_data
        )
    else:
        clean_df = all_clean_data[0]

    return clean_df


ARCGIS_CONFIG = yaml.safe_load(Path('src/county_vitals/arcgis_rest/arcgis_rest_vitals.yaml').read_text())

@asset(
    name="harris",
    group_name="vitals_arcgis",
    key_prefix=['vitals', 'arcgis'],
    metadata={
        "schema": "origin/vitals",
        "table_name": "harris_vitals",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_vitals_harris(context) -> pd.DataFrame:
    return get_vitals_manager(ARCGIS_CONFIG['harris'])


@asset(
    name="travis",
    group_name="vitals_arcgis",
    key_prefix=['vitals', 'arcgis'],
    metadata={
        "schema": "origin/vitals",
        "table_name": "travis_vitals",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_vitals_travis(context) -> pd.DataFrame:
    return get_vitals_manager(ARCGIS_CONFIG['travis'])


@asset(
    name="bexar",
    group_name="vitals_arcgis",
    key_prefix=["vitals", "arcgis"],
    metadata={
        "schema": "origin/vitals",
        "table_name": "bexar_vitals",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_vitals_bexar(context) -> pd.DataFrame:
    return get_vitals_manager(ARCGIS_CONFIG['bexar'])


@asset(
    name="randall",
    group_name="vitals_arcgis",
    key_prefix=['vitals', 'arcgis'],
    metadata={
        "schema": "origin/vitals",
        "table_name": "randall_vitals",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_vitals_randall(context) -> pd.DataFrame:
    return get_vitals_manager(ARCGIS_CONFIG['randall'])


@asset(
    name="potter",
    group_name="vitals_arcgis",
    key_prefix=['vitals', 'arcgis'],
    metadata={
        "schema": "origin/vitals",
        "table_name": "potter_vitals",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_vitals_potter(context) -> pd.DataFrame:
    return get_vitals_manager(ARCGIS_CONFIG['potter'])


@asset(
    name="denton",
    group_name="vitals_arcgis",
    key_prefix=['vitals', 'arcgis'],
    metadata={
        "schema": "origin/vitals",
        "table_name": "denton_vitals",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_vitals_denton(context) -> pd.DataFrame:
    return get_vitals_manager(ARCGIS_CONFIG['denton'])


@asset(
    name="nueces",
    group_name="vitals_arcgis",
    key_prefix=['vitals', 'arcgis'],
    metadata={
        "schema": "origin/vitals",
        "table_name": "nueces_vitals",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def get_vitals_nueces(context) -> pd.DataFrame:
    return get_vitals_manager(ARCGIS_CONFIG['nueces'])
