import json
from datetime import datetime as dt
from pathlib import Path

import pandas as pd
import requests
import yaml


def write_file(df: pd.DataFrame, table_path: str) -> None:
    print(f'Writing file to {table_path}')
    df.to_csv(f'{table_path}.csv', index=False)


def load_csv(url: str) -> pd.DataFrame:
    df = pd.read_csv(url)
    return df


# def get_data_manager(url: str, offsets: list, max_date: int) -> pd.DataFrame | None:
def get_data_manager(config: dict) -> pd.DataFrame | None:
    def get_max_timestamp(config: dict) -> int:
        if config['full_refresh'] and config['file_exists']:
            file_path = f'{config["out"]["dir"]}/{config["out"]["table_name"]}'

            max_file = pd.read_csv(file_path)
            max_date = max_file['Date'].max()
        else:
            max_date = dt.strftime(dt(1999, 12, 31), '%Y-%m-%d')

        max_date_timestamp = int(dt.strptime(max_date, '%Y-%m-%d').timestamp() * 1000)
        return max_date_timestamp

    def get_offsets(request_url: str, step_interval: int) -> list:
        def create_num_records_request(url: str) -> str:
            url_base = url.split('query?')[0]
            url_count_suffix = 'query?where=1%3D1&returnCountOnly=true&f=pjson'
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

        url_location = 'https://services.arcgis.com'
        url_suffix = '&outSR=4326&f=json&resultOffset='

        url_base = f'{url_location}/{owner}/arcgis/rest/services/{source_table}/FeatureServer/0'
        url_query_base = 'query?where=1%3D1&outFields='

        col_list = config['col']['input']
        col_list_formatted = '%2C+'.join(col_list)
        url_query = f'{url_query_base}{col_list_formatted}{url_suffix}'

        url_out = f'{url_base}/{url_query}'

        return url_out

    def get_date_col(config: dict) -> str:
        return ([k for k, v in config['col']['rename'].items() if v == 'Date'][0])

    request_url = create_request_url(config)
    date_col = get_date_col(config)

    max_timestamp = get_max_timestamp(config)
    offsets = get_offsets(
        request_url=request_url,
        step_interval=config['step_interval']
    )

    new_df_list = []
    for offset in offsets:
        print(f'Obtaining data with offset: {offset}')

        df = get_data(request_url, offset)

        # filtering by date/timestamp in rest query wasn't working
        df_new = (df.query(f'{date_col} > @max_timestamp'))
        if df_new.empty:
            break

        new_df_list.append(df_new)

    if not new_df_list:
        return None

    new_df_combined = pd.concat(new_df_list)
    return new_df_combined


def clean_data(df: pd.DataFrame, config) -> pd.DataFrame:
    clean_df = (
        df
        .astype(config['col']['dtypes'])
        .rename(columns=config['col']['rename'])
        .assign(Date=lambda x: pd.to_datetime(x['Date'] * 1_000_000).dt.date)
        .dropna(subset=config['col']['uid'])
        .assign(County=config['county'])
        [config['col']['output']]
    )

    return clean_df


def get_vitals(config: dict) -> None:
    print(f'================Obtaining {config["data_type"]} - {config["county"]}==============================')
    raw_data = get_data_manager(config)

    if raw_data is None:
        print(f'No new data found')
        return None

    clean_df = clean_data(raw_data, config)

    df_out_path = f'{config["out"]["dir"]}/{config["out"]["table_name"]}'
    write_file(clean_df, df_out_path)


CONFIG = yaml.safe_load(Path('src/county_vitals/config/arcgis_rest_vitals.yml').read_text())
counties = list(CONFIG.keys())
config = CONFIG['bexar']
get_vitals(config)

# [get_vitals(conf[county]) for county in counties]
