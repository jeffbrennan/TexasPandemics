import pandas as pd
import json
import requests


def run_diagnostics(df: pd.DataFrame, id_col: str) -> None:
    check_if_id_null = df[id_col].isnull().any()
    assert not check_if_id_null, 'Null id found'

    check_if_date_null = df['Date'].isnull().any()
    assert not check_if_date_null, 'Null date found'


def get_max_timestamp(path: str | None) -> int:
    if path is None:
        max_date = dt.strftime(dt(1999, 12, 31), '%Y-%m-%d')
    else:
        max_date = pd.read_csv(path)['Date'].max()

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
        output = json.loads(response.content)['features'][0]['attributes']['n']
        return output

    num_records_request = create_num_records_request(request_url)
    offsets = list(range(0, get_num_records(num_records_request), step_interval))
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
