from pathlib import Path

import pandas as pd
import requests
import yaml

from src.county_vitals.request_common import clean_request_data
from src.utils import write_file


def parse_response(response: requests.Response, config: dict) -> pd.DataFrame | None:
    if response.status_code != 200:
        raise Exception(f'Error: {response.status_code}')

    response_json = response.json()
    result_df = None
    try:
        result_data_source = response_json['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0']
        result_data_series = pd.DataFrame(result_data_source)['C']
        result_df = pd.DataFrame(result_data_series.tolist())
    except KeyError as e:
        print(f'Error: {e}')

    if config['col']['date_missing']:
        result_df['Date'] = dt.strftime(dt.today(), config['col']['date_format'])

    return result_df


def get_data(config: dict) -> requests.Response:
    response = requests.post(
        url=config['url']['base'],
        headers=config['url']['headers'],
        json=config['url']['payload'],
    )
    return response


def get_vitals(config) -> None:
    print(f'Getting data for {config["county"]} County')
    response = get_data(config)
    result_df = parse_response(response)

    if result_df is not None:
        clean_df = clean_request_data(result_df, config)
        write_file(clean_df, 'tableau/vitals/staging/tarrant_vitals')


CONFIG = yaml.safe_load(Path('src/county_vitals/power_bi/power_bi_config.yaml').read_text())
counties = list(CONFIG.keys())
config = CONFIG[counties[0]]

# [get_vitals(CONFIG[county]) for county in counties]
