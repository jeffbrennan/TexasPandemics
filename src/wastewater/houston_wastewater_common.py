import pandas as pd
import json
import requests


def run_diagnostics(df: pd.DataFrame, id_col: str) -> None:
    check_if_id_null = df[id_col].isnull().any()
    assert not check_if_id_null, 'Null id found'

    check_if_date_null = df['Date'].isnull().any()
    assert not check_if_date_null, 'Null date found'


def get_offsets(request_url: str, step_interval: int) -> list:
    # retrieves value from rest request "count(*) as n"
    def get_num_records(url) -> int:
        response = requests.get(url)
        output = json.loads(response.content)['features'][0]['attributes']['n']
        return output

    offsets = list(range(0, get_num_records(request_url), step_interval))
    return offsets


def get_data_manager(url_prefix, url_suffix, offsets: list, max_date: int) -> pd.DataFrame | None:
    def get_data(url_prefix: str, url_suffix: str, offset: int):
        url = f'{url_prefix}{offset}{url_suffix}'
        request = requests.get(url)
        response = json.loads(request.content)['features']
        df = pd.DataFrame.from_records(i['attributes'] for i in response)
        return df

    new_df_list = []
    for offset in offsets:
        print(f'Obtaining data with offset: {offset}')

        df = get_data(url_prefix, url_suffix, offset)

        # filtering by date/timestamp in rest query wasn't working
        df_new = (df.query('date > @max_date'))
        if df_new.empty:
            break

        new_df_list.append(df_new)

    if not new_df_list:
        return None

    new_df_combined = pd.concat(new_df_list)
    return new_df_combined
