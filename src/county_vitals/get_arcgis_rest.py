import json
from datetime import datetime as dt
from pathlib import Path

import pandas as pd
import requests
import yaml
import time

from src.utils import load_csv, write_file, convert_date
from functools import reduce


def get_data_manager(config: dict) -> pd.DataFrame | None:
    def format_date(max_date_timestamp: int, config: dict) -> int | str:
        if config['col']['date_format'] == 'timestamp_int':
            return max_date_timestamp

        formatted_date = dt.fromtimestamp(max_date_timestamp / 1000).strftime(config['col']['date_format'])
        return formatted_date

    def get_max_date(config: dict) -> int | str:
        if not config['full_refresh'] and config['file_exists']:
            file_path = f'{config["out"]["dir"]}/{config["out"]["table_name"]}'

            max_file = load_csv(file_path)
            max_date = max_file['Date'].max()
        else:
            max_date = dt.strftime(dt(1999, 12, 31), '%Y-%m-%d')

        max_date_timestamp = int(dt.strptime(max_date, '%Y-%m-%d').timestamp() * 1000)
        date_out = format_date(max_date_timestamp, config)
        return date_out

    def get_offsets(request_url: str, step_interval: int) -> list:
        def create_num_records_request(url: str) -> str:
            url_base = url.split('&outFields=')[0]
            url_count_suffix = '&returnCountOnly=true&f=pjson'
            url_out = f'{url_base}{url_count_suffix}'
            return url_out

        # retrieves value from rest request "count(*) as n"
        def get_num_records(url) -> int:
            response = requests.get(url)
            output = json.loads(response.content)['count']
            return output

        num_records_request = create_num_records_request(request_url)
        offsets = list(range(0, get_num_records(num_records_request), step_interval))
        return offsets

    def get_data(url: str, offset: int):
        url = f'{url}{offset}'
        request = requests.get(url)
        response = json.loads(request.content)['features']
        df = pd.DataFrame.from_records(i['attributes'] for i in response)
        return df

    def create_request_url(config) -> str:
        url_config = config['url']
        source_table = url_config['source_table']
        owner = url_config['owner']
        feature_server_id = url_config['feature_server_id']

        url_location = url_config['base']
        url_suffix = '&outSR=4326&f=json&resultOffset='

        url_base_prep = f'{url_location}/{owner}/arcgis/rest/services/{source_table}'
        url_base = f'{url_base_prep}/FeatureServer/{feature_server_id}'

        if 'filter' in config['col']:
            filter_config = config['col']['filter']
            url_query_where = f'where={filter_config["col"]}%3D%27{filter_config["value"]}%27'
        else:
            url_query_where = 'where=1%3D1'

        col_list = config['col']['input']
        col_list_formatted = '%2C+'.join(col_list)
        url_query = f'query?{url_query_where}&outFields={col_list_formatted}{url_suffix}'

        url_out = f'{url_base}/{url_query}'

        return url_out

    def get_date_col(config: dict) -> str:
        return ([k for k, v in config['col']['rename'].items() if v == 'Date'][0])

    request_url = create_request_url(config)
    date_col = get_date_col(config)
    max_date = get_max_date(config)

    offsets = get_offsets(
        request_url=request_url,
        step_interval=config['step_interval']
    )

    new_df_list = []
    for offset in offsets:
        print(f'Obtaining data with offset: {offset}')

        df = get_data(request_url, offset)

        # filtering by date/timestamp in rest query wasn't working
        df_new = (df.query(f'{date_col} > @max_date'))
        if df_new.empty:
            break

        new_df_list.append(df_new)

    if not new_df_list:
        return None

    new_df_combined = pd.concat(new_df_list)
    return new_df_combined


def clean_data(df: pd.DataFrame, config) -> pd.DataFrame:
    agg_dict = {col: 'sum' for col in config['col']['metric_out']}

    clean_df = (
        df
        .astype(config['col']['dtypes'])
        .rename(columns=config['col']['rename'])
        .assign(
            Date=lambda x: convert_date(
                date_series=x['Date'],
                date_format=config['col']['date_format'])
        )
        .dropna(subset=config['col']['uid'])
        .assign(County=config['county'])
        .groupby(['County', 'Date'], as_index=False)
        .agg(agg_dict)
    )

    clean_df = clean_df[config['col']['output']]

    return clean_df


def parse_data_manager(config: dict) -> pd.DataFrame:
    attempts = 0
    while attempts < 5:
        try:
            raw_data = get_data_manager(config)
            break
        except KeyError:
            attempts += 1
            print(f'Attempt {attempts} failed')
            time.sleep(2 * 60)
            continue

    if raw_data is None:
        print(f'No new data found')
        return None

    clean_df_raw = clean_data(raw_data, config)
    return clean_df_raw


def create_config_list(config: dict) -> list:
    if list(config['col'].keys()) != ['cases', 'deaths']:
        return [config]

    # handle data structures where county, case, and death dates are in different columns (amarillo ???)
    config_cases = config.copy()
    config_cases['col'] = config['col']['cases']

    config_deaths = config.copy()
    config_deaths['col'] = config['col']['deaths']

    return [config_cases, config_deaths]


def get_vitals(config: dict) -> None:
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

    df_out_path = f'{config["out"]["dir"]}/{config["out"]["table_name"]}'
    write_file(clean_df, df_out_path)


CONFIG = yaml.safe_load(Path('src/county_vitals/config/arcgis_rest_vitals.yaml').read_text())
counties = list(CONFIG.keys())
[get_vitals(CONFIG[county]) for county in counties]
